# End-to-end benchmark against Deephaven

How fast Oxhaven runs table operations compared with the Java reference, Deephaven 42.9, both
driven the same way by a client over gRPC. Task 8.1 (`tasks/8.1-benchmark.md`) built the
runner, `bench/bench.py`, and made the first measurement below. The only earlier measurement is
the row set micro-benchmark in [ROWSET.md](ROWSET.md).

The short reading: every case gave the same answer on both servers. At 1M rows Oxhaven is
within about 2x of Java in the less loaded run (up to 4x in the other) on formulas, filters, low- and medium-cardinality aggregations,
`join`, `head`/`tail` and `ungroup`, and faster on the Arrow Flight read path and on a string
match filter. It is 18x to 40x slower on `natural_join` and `exact_join`, 8x to 31x on
`select_distinct`, 4x to 10x on `sort` of an int column and on `last_by`, and 3x to 7x on
high-cardinality aggregations and `group_by`. At 10M rows the formulas and filters fall to 3x
to 8x, mostly because Java runs them on 2 threads. The slow paths share one cause: they keep
per-row state in ordered trees of heap-allocated keys (`BTreeMap`, `BTreeSet`, boxed byte
strings) where Java uses flat arrays and open-addressed hash tables. Oxhaven was written
correct-first (PORTING.md, rule 5) and has never been tuned; Java has. The machine was shared
with other jobs during both runs, so ratios under about 2x are within the noise.

Since then, task 9.1 has made `natural_join` and `exact_join` 14x to 88x faster than they
were in this first measurement, which brings them to Java's times or below them; see
[After task 9.1](#after-task-91-natural-and-exact-joins-2026-11-17). Task 9.3 has made
`sort` 3x to 19x faster, below Java's earlier times at both sizes; see
[After task 9.3](#after-task-93-2026-11-17-sort-and-reads-through-a-row-redirection). The tables and the
reading of the first measurement are kept below as they were written. Task 9.8 measured
both servers again after tasks 9.1 to 9.7; see
[Second measurement](#second-measurement-after-tasks-91-to-97) at the end. Both
measurements held Deephaven to 2 processors while Oxhaven used 4 threads; task 9.11
measured both on 4 and both on one: see [Third measurement](#third-measurement-deephaven-on-four-processors-and-both-on-one-thread).

## What is measured

One **case** is an operation on a table that lives on the server. A case runs in three parts:

1. **Setup**, not timed: the case's input is made on the server. For every static case it is a
   fresh `head(n)` of the source table, so that no repetition reuses another's result: Java
   memoizes `where`, `sort`, aggregations and `select` of plain columns per parent table
   (`QueryTable#memoizeResult`), and `head` (`SliceLikeOperation`) is not memoized.
2. **The operation**, timed at the client with a monotonic clock from the request to its
   completion, as below.
3. **The check**, not timed, after the last repetition: the result's row count, the sum of each
   numeric column, a sum of the first numeric column weighted by row position modulo 1009 (so
   that row order counts), and the first and last three rows. The case is reported as the same
   result only when every part agrees on both servers (floating point sums within a relative
   1e-9).

Each case runs `warm-up` untimed repetitions and then the timed ones; the table reports the
median, minimum and maximum of the timed ones. The cases run interleaved: case 1 on Java, case
1 on Oxhaven, case 2 on Java, and so on, so that a change of the machine's load affects both.

**When an operation is complete.** For every case that returns a table, the client call
returns when the server has answered the export with `ExportedTableCreationResponse`, which
carries the result's size. Both servers send it only after the operation has made its result
table. What "made" means is the same on both: `update` stores its formula columns (Java
`QueryTable#update`, Oxhaven `ops::select`), and filters, sorts, aggregations and joins compute
their row sets, permutations, groups and redirections before they answer; the values of a
sorted or joined column are read through a redirection when someone reads them, on both. The
other two kinds of case say what they wait for in the table below.

| case | input (setup) | timed operation | complete when |
| --- | --- | --- | --- |
| `build` | none | `empty_table(n).update(` the 7 source formulas `)` | export answered |
| `update_arith` | source | `update(["C = A * 2 + 1", "D = B * 1.5 - A", "E = Id % 7"])` | export answered |
| `update_string` | source | `update(['S2 = S + "_x"'])` | export answered |
| `update_cond` | source | `update(["F = A > 50000 ? B : -B"])` | export answered |
| `where_range` | source | `where("A >= 20000 && A < 60000")` (40% of rows) | export answered |
| `where_string` | source | `` where("S in `s1`, `s42`, `s999`") `` (0.3%) | export answered |
| `where_conj` | source | `where(["K10 = 3", "B > 50000.0", "A < 50000"])` | export answered |
| `sort_int`, `sort_two`, `sort_string` | source | `sort("A")`, `sort(["K10", "B"])`, `sort("S")` | export answered |
| `sum_by_*`, `avg_by_*`, `count_by_*` | `view(["K = <key>", "A", "B"])` of the source | `sum_by("K")`, `avg_by("K")`, `count_by("N", "K")` | export answered |
| `agg_by_multi` | source | `agg_by([sum, avg, min, max, count], by="K10k")` | export answered |
| `last_by` | source | `last_by("K10k")` (all 7 columns) | export answered |
| `natural_join_small`, `exact_join_small` | source | join on `K10k` to a right table of 10,000 unique keys | export answered |
| `natural_join_large`, `exact_join_large` | source | join on `K1M` to a right table of 1,000,000 unique keys | export answered |
| `join_on_key` | source | `join(right, on="K10k", joins="R1")`, right table of 10,000 unique keys | export answered |
| `aj` | source | `aj(right, on=["K10", "Id >= Stamp"])`, right table of n/10 rows | export answered |
| `select_distinct_one`, `select_distinct_two` | source | `select_distinct("K10k")`, `select_distinct(["K10", "S"])` (10,000 groups each) | export answered |
| `head_half`, `tail_half` | source | `head(n/2)`, `tail(n/2)` | export answered |
| `group_by` | `view(["K10k", "A"])` of the source | `group_by("K10k")` | export answered |
| `ungroup` | `view(["K10k", "A"]).group_by("K10k")`, made once | `ungroup("A")` (n rows back) | export answered |
| `doget_1m` | the first 1M rows of `Id`, `A`, `B` | `to_arrow()`: Arrow Flight DoGet | the client has every record batch |
| `ticking_append_where_sum_by` | an append-only input table `(K int, V long)` with `where("V % 2 == 0").sum_by("K")` downstream; a batch of 100,000 rows imported | add the batch, wait for the barrier, fetch the 100-row result | see "Ticking" |
| `sort_refreshing_append` (added by task 9.8) | once: an append-only input table `(Id long, A int, B double)` given the source's n rows on the server, then `sort("A")` of it; per repetition a batch of 1,000 new rows (the source's formulas for the next ids) imported | add the batch, wait for the barrier | the cycle that sorted the batch in has finished; checked on a `snapshot()` of the sorted table |
| `doget_sorted_refreshing` (added by task 9.8) | `head(1_000_000)` of that sorted refreshing table | `to_arrow()`: Arrow Flight DoGet | the client has every record batch; the check also weighs the first column by row position |

The source table has n rows:

```
Id   = ii                                    long, 0..n-1
K10  = (int)(ii % 10)                        10 keys
K10k = (int)((ii * 7919) % 10000)            10,000 keys, spread
K1M  = (int)((ii * 104729) % 1000000)        1,000,000 keys, spread (all unique at n = 1M)
A    = (int)((ii * 31337) % 100003)          int
B    = ((ii * 15485863) % 1000003) / 7.0     double
S    = "s" + (ii % 1000)                     string, 1,000 values
```

"low", "med" and "high" in the aggregation case names are `K10`, `K10k` and `K1M`.

### Ticking

One ticking case only, because it is the one that both servers can be driven through
identically and whose end can be observed from the client without timing the update graph's
clock. An input table without keys gets a fixed batch of 100,000 rows per repetition (imported
before the timer starts), and a `where` + `sum_by` over 100 keys is kept up to date downstream.
The timer covers the `AddTableToInputTable` call, then two changes to a second, private input
table (the oracle's barrier, `_Barrier` in `oracle/harness/runner.py`: when the second returns,
the cycle that applied the batch has finished on both servers), then a DoGet of the 100-row
result. The check compares the fetched sums with the sums the client computes for every row
added so far. Both servers start a cycle as soon as an input table is changed
(`PeriodicUpdateGraph#requestRefresh` and its Oxhaven counterpart), so the time is not the
1-second cycle period. It is dominated by round trips and cycle overhead, not by the 100,000-row
`where` and `sum_by`, so it says little about incremental throughput; a `time_table` was left
out because nothing can make the two servers tick in step. Task 9.8 added a second case of
the same kind, `sort_refreshing_append`: 1,000 rows appended per repetition to an input
table of n rows that a `sort` keeps ordered, timed to the barrier.

## How to run it

```bash
# Python environment: the oracle's pins (oracle/requirements.txt), in a venv anywhere.
python3 -m venv /tmp/bench-venv && /tmp/bench-venv/bin/pip install -r oracle/requirements.txt

# Java: the oracle's own way (oracle/bin/start-java.sh), here with a larger heap for 10M rows.
ORACLE_HEAP=5g oracle/bin/start-java.sh 10868          # needs oracle/.venv; or run
# oracle/bin/java_server.py --port 10868 --psk oxhaven --heap 5g with the venv's python

# Oxhaven: a release build, without the Python console (the cases need no console).
cargo build --release -p oxhaven-server
target/release/oxhaven-server --host 127.0.0.1 --port 10869 --psk oxhaven &

/tmp/bench-venv/bin/python bench/bench.py run \
    --server java=dh+psk://oxhaven@127.0.0.1:10868 \
    --server oxhaven=dh+psk://oxhaven@127.0.0.1:10869 \
    --out run1.json                       # --sizes, --warmup, --reps, --cases 'sort_*,aj'
/tmp/bench-venv/bin/python bench/bench.py report run1.json run2.json            # min-max, load
/tmp/bench-venv/bin/python bench/bench.py report --compact run1.json run2.json  # medians only
```

The first `--server` is the base of the ratio. The defaults are 1M and 10M rows, 2 warm-up
and 5 timed repetitions at 1M rows, 1 and 3 at 10M rows; one run took 14 to 15 minutes on the
machine below. A failed case is recorded, not dropped: `refused` for `UNIMPLEMENTED` or
`INVALID_ARGUMENT`, `error` otherwise, with the code and message in the JSON.

## The machine and the servers

Measured on 2026-11-17, two complete runs one after the other (run 1 10:36 to 10:51, run 2
10:52 to 11:06), on the same two server processes, started once before run 1.

- **Machine:** 4 vCPUs (Intel Xeon @ 2.10 GHz), 15 GB RAM, Linux 6.18. Client and both
  servers on the same host, over loopback.
- **Load: not clean.** Other agents shared the machine throughout: an engine test build and its
  test binary (up to 3 cores), and for part of the time a fuzzer against another Java server.
  The one-minute load average, recorded before each case, was 2.9 to 3.9 during the 1M-row cases
  of run 1, 4.2 to 8.2 during those of run 2, and 3.4 to 9.2 during the 10M-row cases of both
  runs; the benchmark itself, one busy server and the client, accounts for one to two of that. With 4 cores, a single-threaded operation got
  between half a core and a whole one. Read differences under about 1.5x between the runs, and
  ratios under about 2x, as noise. The full table (`bench/results/2026-11-17.md`) has the
  min-max of every case and the load for each case and run.
- **Java:** Deephaven 42.9, the `deephaven-server` wheel of `oracle/requirements.txt`, started
  as the oracle starts it (`oracle/bin/java_server.py`: `-XX:ActiveProcessorCount=2`, PSK
  authentication) but with a 5 GB heap (`--heap 5g`) instead of the oracle's 2 GB, chosen for the 10M-row
  tables; the resident size of the Java process reached 4.6 GB during them. 2 GB was not tried. OpenJDK 21.0.12.1 with its default collector, Deephaven's default
  configuration otherwise, so its operation thread pool and update graph have 2 threads.
  For Deephaven on 4 processors and on one, see [Third measurement](#third-measurement-deephaven-on-four-processors-and-both-on-one-thread).
- **Oxhaven:** commit `c24fd2a2` (main at `6f744d8f` plus the runner), built with
  `cargo build --release --locked -p oxhaven-server` and the default release profile (no LTO,
  debug assertions off), without the `python` feature, run with default options
  (`--cycle-ms 1000`). The gRPC runtime is Tokio's default multi-threaded one (7 threads in the
  process on 4 cores); the engine has no parallel code, so a table operation computes on one thread.
- **Client:** pydeephaven 42.9 and pyarrow 26.0.1 on Python 3.13, sessions opened with
  `oracle/harness/connect.py`.
- **Repetitions:** 2 warm-up and 5 timed at 1M rows; 1 warm-up and 3 timed at 10M rows.

## Results

Every case gave the same result on both servers in both runs; no case was refused and none
failed.

Median milliseconds of each run, run 1 / run 2. Ratio = oxhaven / java; above 1 means oxhaven is slower.

### 1,000,000 rows

| case | java | oxhaven | ratio | same result |
| --- | ---: | ---: | ---: | --- |
| `build` | 173 / 139 | 328 / 397 | 1.90 / 2.87 | yes / yes |
| `update_arith` | 43.7 / 31.8 | 54.9 / 73.5 | 1.26 / 2.31 | yes / yes |
| `update_string` | 100 / 52.8 | 172 / 209 | 1.72 / 3.96 | yes / yes |
| `update_cond` | 17.7 / 26.5 | 24.6 / 25.1 | 1.39 / 0.95 | yes / yes |
| `where_range` | 32.0 / 15.0 | 28.9 / 42.3 | 0.90 / 2.81 | yes / yes |
| `where_string` | 16.6 / 18.3 | 8.9 / 9.5 | 0.54 / 0.52 | yes / yes |
| `where_conj` | 17.9 / 14.3 | 21.9 / 24.3 | 1.22 / 1.70 | yes / yes |
| `sort_int` | 116 / 153 | 607 / 619 | 5.25 / 4.04 | yes / yes |
| `sort_two` | 232 / 251 | 529 / 623 | 2.28 / 2.48 | yes / yes |
| `sort_string` | 280 / 317 | 416 / 451 | 1.48 / 1.42 | yes / yes |
| `sum_by_low` | 20.1 / 18.9 | 38.2 / 37.2 | 1.89 / 1.97 | yes / yes |
| `avg_by_low` | 21.2 / 20.1 | 33.5 / 36.8 | 1.58 / 1.83 | yes / yes |
| `count_by_low` | 13.6 / 17.7 | 27.6 / 38.2 | 2.03 / 2.16 | yes / yes |
| `sum_by_med` | 47.9 / 50.5 | 60.6 / 88.5 | 1.26 / 1.75 | yes / yes |
| `avg_by_med` | 53.6 / 60.7 | 81.2 / 91.5 | 1.52 / 1.51 | yes / yes |
| `count_by_med` | 20.6 / 18.3 | 45.8 / 67.3 | 2.22 / 3.68 | yes / yes |
| `sum_by_high` | 183 / 194 | 1126 / 1080 | 6.17 / 5.58 | yes / yes |
| `avg_by_high` | 194 / 199 | 1018 / 1161 | 5.25 / 5.83 | yes / yes |
| `count_by_high` | 106 / 133 | 807 / 927 | 7.62 / 6.98 | yes / yes |
| `agg_by_multi` | 76.2 / 103 | 124 / 130 | 1.62 / 1.27 | yes / yes |
| `last_by` | 20.8 / 23.0 | 207 / 193 | 9.95 / 8.36 | yes / yes |
| `natural_join_small` | 28.7 / 25.4 | 655 / 699 | 22.82 / 27.58 | yes / yes |
| `natural_join_large` | 125 / 125 | 2325 / 2578 | 18.57 / 20.59 | yes / yes |
| `exact_join_small` | 25.9 / 24.7 | 680 / 687 | 26.30 / 27.86 | yes / yes |
| `exact_join_large` | 140 / 123 | 2575 / 2455 | 18.45 / 19.92 | yes / yes |
| `join_on_key` | 365 / 314 | 594 / 586 | 1.63 / 1.87 | yes / yes |
| `aj` | 213 / 185 | 603 / 535 | 2.82 / 2.89 | yes / yes |
| `select_distinct_one` | 16.6 / 13.8 | 323 / 276 | 19.50 / 19.96 | yes / yes |
| `select_distinct_two` | 30.4 / 29.5 | 285 / 249 | 9.36 / 8.44 | yes / yes |
| `head_half` | 2.0 / 1.8 | 0.87 / 0.91 | 0.43 / 0.51 | yes / yes |
| `tail_half` | 2.3 / 1.9 | 1.1 / 1.0 | 0.47 / 0.54 | yes / yes |
| `group_by` | 109 / 79.6 | 366 / 314 | 3.35 / 3.94 | yes / yes |
| `ungroup` | 5.7 / 2.5 | 2.0 / 3.9 | 0.34 / 1.54 | yes / yes |
| `doget_1m` | 73.9 / 64.7 | 29.6 / 29.6 | 0.40 / 0.46 | yes / yes |
| `ticking_append_where_sum_by` | 46.9 / 54.9 | 79.6 / 96.7 | 1.70 / 1.76 | yes / yes |

### 10,000,000 rows

| case | java | oxhaven | ratio | same result |
| --- | ---: | ---: | ---: | --- |
| `build` | 1738 / 2577 | 5398 / 5397 | 3.11 / 2.09 | yes / yes |
| `update_arith` | 179 / 355 | 1050 / 993 | 5.87 / 2.80 | yes / yes |
| `update_string` | 1094 / 1460 | 2145 / 2145 | 1.96 / 1.47 | yes / yes |
| `update_cond` | 71.9 / 89.7 | 392 / 401 | 5.46 / 4.47 | yes / yes |
| `where_range` | 52.1 / 48.5 | 428 / 402 | 8.22 / 8.29 | yes / yes |
| `where_string` | 121 / 161 | 82.0 / 96.2 | 0.68 / 0.60 | yes / yes |
| `where_conj` | 92.9 / 80.3 | 274 / 267 | 2.95 / 3.33 | yes / yes |
| `sort_int` | 1175 / 1255 | 11775 / 11292 | 10.02 / 9.00 | yes / yes |
| `sort_two` | 2779 / 2810 | 11711 / 11201 | 4.21 / 3.99 | yes / yes |
| `sort_string` | 2546 / 2372 | 6837 / 6652 | 2.69 / 2.80 | yes / yes |
| `sum_by_low` | 166 / 166 | 332 / 288 | 2.00 / 1.73 | yes / yes |
| `avg_by_low` | 205 / 158 | 325 / 332 | 1.59 / 2.11 | yes / yes |
| `count_by_low` | 144 / 103 | 329 / 226 | 2.29 / 2.18 | yes / yes |
| `sum_by_med` | 589 / 379 | 817 / 550 | 1.39 / 1.45 | yes / yes |
| `avg_by_med` | 574 / 480 | 892 / 692 | 1.55 / 1.44 | yes / yes |
| `count_by_med` | 265 / 150 | 469 / 351 | 1.77 / 2.35 | yes / yes |
| `sum_by_high` | 912 / 871 | 3496 / 2813 | 3.84 / 3.23 | yes / yes |
| `avg_by_high` | 1392 / 1021 | 3360 / 2761 | 2.41 / 2.70 | yes / yes |
| `count_by_high` | 498 / 437 | 2691 / 2448 | 5.40 / 5.60 | yes / yes |
| `agg_by_multi` | 882 / 743 | 1353 / 1409 | 1.53 / 1.89 | yes / yes |
| `last_by` | 258 / 221 | 2120 / 1928 | 8.20 / 8.73 | yes / yes |
| `natural_join_small` | 344 / 297 | 9510 / 8522 | 27.62 / 28.70 | yes / yes |
| `natural_join_large` | 584 / 514 | 20378 / 17393 | 34.90 / 33.84 | yes / yes |
| `exact_join_small` | 243 / 212 | 9804 / 7920 | 40.39 / 37.30 | yes / yes |
| `exact_join_large` | 674 / 500 | 19910 / 16681 | 29.56 / 33.39 | yes / yes |
| `join_on_key` | 3982 / 2355 | 7532 / 6350 | 1.89 / 2.70 | yes / yes |
| `aj` | 2677 / 1822 | 6570 / 5729 | 2.45 / 3.14 | yes / yes |
| `select_distinct_one` | 103 / 100 | 3201 / 2767 | 31.15 / 27.58 | yes / yes |
| `select_distinct_two` | 321 / 249 | 2680 / 2385 | 8.34 / 9.56 | yes / yes |
| `head_half` | 2.7 / 3.6 | 0.85 / 4.0 | 0.32 / 1.10 | yes / yes |
| `tail_half` | 1.7 / 1.5 | 4.0 / 4.2 | 2.40 / 2.88 | yes / yes |
| `group_by` | 1209 / 934 | 8797 / 6709 | 7.28 / 7.19 | yes / yes |
| `ungroup` | 2.9 / 2.3 | 17.8 / 9.2 | 6.15 / 4.06 | yes / yes |
| `doget_1m` | 88.7 / 70.5 | 39.1 / 44.2 | 0.44 / 0.63 | yes / yes |
| `ticking_append_where_sum_by` | 51.4 / 39.6 | 113 / 96.4 | 2.21 / 2.44 | yes / yes |

## After task 9.1: natural and exact joins (2026-11-17)

Task 9.1 (`tasks/9.1-fast-joins.md`) rewrote how `natural_join` and `exact_join` find their
matches and how a result's row redirection is stored. This block measures that change only.

**What was compared.** Two Oxhaven servers on the machine above, run by the same runner with
the join cases only (`--cases 'natural_join_*,exact_join_*,join_on_key,aj'`), interleaved case
by case: "before" is `main` at `03f8dbb8`, "after" is the branch of task 9.1, both built with
`cargo build --release --locked -p oxhaven-server`. Two complete runs, 12:08 to 12:15 and
12:15 to 12:23, with other agents building on the machine (one-minute load 2.7 to 4.4 in run
1 and 5.0 to 7.6 in run 2). Every case gave the same result on both servers in both runs.
Java was not run again: the "java (earlier)" column repeats the medians of the first
measurement above, taken two hours earlier under another load, so the last column is an
indication and not a measurement of one run. The raw results and the table with min-max and
load per case are in `bench/results/2026-11-17-task-9.1*`.

Median milliseconds of each run, run 1 / run 2.

| case | rows | before | after | before / after | java (earlier) | after / java (earlier) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `natural_join_small` | 1M | 692 / 790 | 7.9 / 10.5 | 88 / 75 | 28.7 / 25.4 | 0.3 to 0.4 |
| `natural_join_large` | 1M | 2286 / 2521 | 122 / 180 | 19 / 14 | 125 / 125 | 1.0 to 1.4 |
| `exact_join_small` | 1M | 640 / 705 | 8.4 / 8.8 | 76 / 80 | 25.9 / 24.7 | 0.3 to 0.4 |
| `exact_join_large` | 1M | 2115 / 2614 | 139 / 154 | 15 / 17 | 140 / 123 | 1.0 to 1.3 |
| `join_on_key` | 1M | 536 / 645 | 536 / 671 | 1.0 / 1.0 | 365 / 314 | 1.5 to 2.1 |
| `aj` | 1M | 474 / 595 | 441 / 475 | 1.1 / 1.3 | 213 / 185 | 2.1 to 2.6 |
| `natural_join_small` | 10M | 7352 / 8528 | 146 / 194 | 50 / 44 | 344 / 297 | 0.4 to 0.7 |
| `natural_join_large` | 10M | 16780 / 18570 | 558 / 698 | 30 / 27 | 584 / 514 | 1.0 to 1.4 |
| `exact_join_small` | 10M | 8068 / 8804 | 161 / 174 | 50 / 51 | 243 / 212 | 0.7 to 0.8 |
| `exact_join_large` | 10M | 15182 / 18875 | 581 / 643 | 26 / 29 | 674 / 500 | 0.9 to 1.3 |
| `join_on_key` | 10M | 6514 / 7448 | 6400 / 6903 | 1.0 / 1.1 | 3982 / 2355 | 1.6 to 2.9 |
| `aj` | 10M | 6356 / 6518 | 5326 / 5667 | 1.2 / 1.2 | 2677 / 1822 | 2.0 to 3.1 |

**What changed.**

- The keys of both tables are numbered by a hash table, a chunk of key columns at a time
  (`ops/join/key_table.rs`): a single primitive, boolean or time column as 64-bit integers kept
  in the table itself, a single string column as bytes borrowed from the chunk, and any other
  combination as rows of Arrow's row format. No key is allocated per row, and the hash has a
  fixed seed.
- A join of two static tables makes one pass over each table and keeps nothing per key or per
  row but the redirection. With a refreshing left table and a static right table it keeps the
  first and last right row per key. Only a refreshing right table makes it keep the left rows
  of each key, and a key with one row costs no set.
- `WritableRowRedirection` (`oxhaven-column/src/redirect.rs`) keeps its mappings in a flat
  array indexed by row key while the keys are dense (at most four array cells per mapping, the
  factor of Java's `JoinControl#getRedirectionType`) and in an ordered map otherwise, and moves
  between the two by itself; previous mappings are kept apart for the cycle, as in Java's
  `ContiguousWritableRowRedirection`. A whole result's redirection is made in one piece, and for
  the row keys `0..n` it is the vector of right row keys itself. Every operator that uses a
  redirection gets the array without a change of its own, which is why `aj` is 7% to 20%
  faster here.

**Where the time went, and where it goes now.** The first measurement's reading of the cause
was right in kind. Measured with `cargo run --release -p oxhaven-engine --example join-phases`
on the same machine (one run of the code before and two of the code after, at a load of 4.6 to 7.8), for 10M left rows and 10,000 right keys:

| part | before | after |
| --- | ---: | ---: |
| read the left keys into one `Vec<Cell>` per row | 1047 ms | none |
| left rows into the ordered map and the per-key sets | 5355 ms | none |
| look up the left keys in the hash table | none | 95 to 133 ms |
| right row of each left row | (in the map time) | 50 to 58 ms |
| the redirection | 762 ms (10M `BTreeMap` inserts) | 14 to 16 ms (one vector) |
| the whole join | 7632 ms | 214 to 229 ms |

So the ordered map with its sets was 70% of the time, the keys 14% and the redirection 10%.
The other shapes of the example, whole joins of 10M left rows, before and after: 1,000,000
right keys 15931 ms and 828 to 1255 ms; a string key with 1,000 values 6196 ms and 175 to
255 ms; an int and a string key together (the row-format path) 10631 ms and 471 to 490 ms; a
refreshing left table 7608 ms and 519 to 546 ms; a refreshing right table 7367 ms and 328 to
390 ms, and ten cycles that each add 100 right rows and so change 100,000 result rows 1368 ms
and 325 to 373 ms. In a debug build the 1M-row join with 10,000 right keys went from 2416 ms
to 185 ms and the one with 1,000,000 right keys from 7438 ms to 1400 ms.

**What did not get faster.** `join_on_key` is unchanged: the cross join (`ops/crossjoin`) has
its own keyed state and still reads a `Vec<Cell>` per row. `aj` gained only what the flat
redirection gives it. The times above are for making the join; reading an added column of all
10M rows back through the redirection takes 256 to 331 ms (666 to 737 ms before), because
`RedirectedSource` gathers every row key into a vector and then takes the values one by one.

**The next bottlenecks of these joins.** With 1,000,000 right keys, most of the time
is the lookup of 10M left keys in a table that no longer fits in cache and the second
random read, of the key's right row, that follows it; Java's time is the same within the noise, so there is
little left without a change of method (partitioning the keys, or prefetching). For a
refreshing right table it is the set of left rows per key (a `BTreeSet` each) and one lock and
one ordered-map insertion per changed row of the redirection. A join's key table also never
forgets a key, as Java's does not; the ordered map it replaces dropped a key when its last row
left.

## Reading the results

No profiler was available on the machine (`perf` is not installed), so the causes below come
from reading the code path of each slow case, not from a profile. They are the likely
reasons, not measured ones.

**Close to Java, or faster (ratio below about 2 in run 1, the less loaded run at 1M rows,
and below 4 in run 2):** the formulas of `update` at 1M rows, `where_string` (a match filter
on the string column, faster on Oxhaven at both sizes), low and medium cardinality `sum_by`/`avg_by`/`count_by`, `agg_by_multi`,
`sort_string` at 1M rows, `join_on_key`, `head`/`tail`, and `doget_1m`: Oxhaven sends 1M rows
over Arrow Flight in about 30-45 ms against Java's 65-90 ms, because its columns already are
Arrow arrays and Java converts its chunks to Arrow buffers. The ticking case is 1.7x to 2.4x,
40-55 ms against 80-115 ms, most of which is round trips and cycle overhead on both.

**Slower, and why.** The worst cases, slowest first:

1. **`natural_join` and `exact_join`: 18x to 40x.** (Fixed by task 9.1; see "After task
   9.1" above. What follows describes the code before it.) `ops/join/natural.rs` (`NaturalJoin::start`)
   reads every row's key into a `Vec<Cell>` (`ops/join/keys.rs`: one heap allocation per row,
   with an enum per cell), keeps the keys in a `BTreeMap<Vec<Cell>, Slot>` in which every slot
   has a `BTreeSet` of its left rows and one of its right rows, clones the key once more per
   left row, and puts each left row's match into a `WritableRowRedirection`
   (`oxhaven-column/src/redirect.rs`), which is a `BTreeMap<RowKey, RowKey>` with a second
   map for previous values (`Tracked`). So a 10M-row static join is about 10M allocations, 20M
   ordered-tree insertions of variable-length keys and 10M more into the redirection. Java hashes
   the typed key chunk into an open-addressed table generated per key type and writes the
   redirection into a flat array, and does not keep the left rows per key for a static join.
   The per-left-row state is needed for a refreshing join only.
2. **`select_distinct`: 8x to 31x.** `ops/distinct/mod.rs` turns each row's key into an owned
   `Vec<u8>` (Arrow's row format) and looks it up in a `BTreeMap<Vec<u8>, usize>`, and records
   the group in a `BTreeMap` of touched groups, for every row, also when the table is static.
   Two ordered-tree operations and one allocation per row against Java's hash probe on a typed
   chunk.
3. **`sort`: 4x to 10x, and growing faster than the row count** (`sort_int` 0.6 s at 1M rows,
   11.5 s at 10M). `ops/sort/sorted.rs` (`sorted_entries`) encodes each row's key into its own
   `Box<[u8]>` (`ops/sort/keys.rs`), sorts a vector of (row key, box) entries with a stable
   comparison sort over the boxed bytes, so each comparison follows two pointers to memory that
   is not in cache, and then writes the permutation into a `WritableRowRedirection`, a
   `BTreeMap`, one insertion per row. Java sorts a typed chunk of values with the row keys
   alongside (a timsort per primitive type) and keeps the permutation in a flat array. The
   superlinear growth is what pointer chasing through 10M boxes and a 10M-entry tree do once
   they no longer fit in cache.
4. **`last_by`, `group_by`, and aggregations with 1M keys: 3x to 10x.** The aggregation
   engine (`ops/agg/aggregator.rs`) is chunked like Java's, but each chunk's keys are encoded with
   Arrow's `RowConverter` and looked up in a `HashMap<Box<[u8]>, usize>` with the standard
   library's SipHash (`ops/agg/keys.rs`), a new key costs an allocation, and the operators are
   called once per run of equal slots through a trait object, which costs most when runs are a
   row or two long, as with 1M keys. Every result column is a `WritableSource`
   (`oxhaven-column/src/writable.rs`), whose cells are again entries of a `BTreeMap` keyed by
   row key, so 1M groups is 1M tree insertions per result column. `last_by` keeps the last value
   of each of its 6 value columns per group as a `Scalar` (`ops/agg/first_last.rs`), which for
   a string is a new `Arc<str>` per run. `group_by` does more than Java at creation: it gathers
   every value of every group into one Arrow list array (`ops/agg/group.rs`, `recompute` and
   `results`), where Java's grouped columns are vectors read through each group's row set when
   someone reads them. That is a difference of what "complete" means for this one case, in
   Java's favour; it is also what Oxhaven's later reads of the column do not pay.

**Larger at 10M rows than at 1M.** `update_arith`, `update_cond` and `where_range` are within
about 1.5x of Java at 1M rows but 3x to 8x at 10M. Two reasons, both expected: Java parallelises
`where` over 2^16-row segments and `update` above 2^22 rows on its 2 threads, which pays only at
10M; and Oxhaven evaluates a formula or condition over the whole selection in one call
(`ops/filter/filters.rs`, the `Resolved::Condition` arm), so each intermediate array is 10M
values long and is written to and read back from memory, where Java's kernels work in chunks of
4,096 that stay in cache. The 10M-row `build` (3x) is the same formulas plus the string column.

**`ungroup` and `tail_half` at 10M rows** are 4x to 6x but a few milliseconds on either server;
at that size round-trip noise is a large part of the number.

## Where the comparison is unfair

In Java's favour:

- **Parallelism.** Java uses more than one core for some static operations: a `where` over more
  than 2^16 rows is split into segments filtered in parallel
  (`QueryTable.parallelWhereRowsPerSegment`), and `select`/`update` over more than 2^22 rows
  are split across threads (`QueryTable.minimumParallelSelectRows`). The oracle starts Java with
  `-XX:ActiveProcessorCount=2`, so it has 2 threads for this. Every Oxhaven operation runs on
  one thread. This is part of why `update_*` and `where_range` grow less than linearly from 1M to
  10M rows on Java and linearly on Oxhaven. (Oxhaven has since gained parallel code; for
  both servers on 4 processors and both on one, see [Third measurement](#third-measurement-deephaven-on-four-processors-and-both-on-one-thread).)
- **Tuning.** Java's operators have years of performance work: type-specialised kernels,
  open-addressed hash tables generated per key type, typed sort kernels, flat
  redirections. Oxhaven's are first ports written for correctness and have never been
  profiled.

In Oxhaven's favour:

- **JIT warm-up is excluded.** Two untimed repetitions per case at 1M rows (one at 10M), on a
  JVM that by the second run has executed every case several times, so Java's numbers are its
  steady state. Java's first, cold run of an operation is slower than what the table shows.
- **Memory.** Java keeps strings as objects and pays for garbage collection (5 GB heap here);
  the timing of a collection falls into whichever case triggers it, which is part of Java's
  wider min-max spread. Oxhaven's Arrow arrays are smaller. Memory was not measured.
- **Release build.** Oxhaven was built with `--release`, so `debug_assert!`s are off. The
  self-consistency checker of the engine tests is not in the server either way.

Neutral: both servers run on one machine with the client, over loopback, with the same client
library; the round trip (a few hundred microseconds) is in every number, which matters only for
the cases under about 10 ms (`head_half`, `tail_half`, `ungroup`).

## What was not measured

- Memory: neither the servers' peak nor their resident size per row.
- Ticking throughput under load: many cycles per second, large or wide updates, many
  downstream tables, modifies and shifts (the one ticking case only appends), and Barrage
  subscriptions to ticking tables.
- Tables read from storage (Parquet, CSV) and Kafka ingestion.
- `update_by`, range joins, rollups and trees, partitioned tables, formulas calling Python, and
  the Python console in general.
- Concurrency: several clients or several operations at once.
- Cold JVM start, and sizes above 10M rows.

## Follow-ups

Ordered by value for the effort; estimates are for one agent and include tests against the
oracle corpus and the self-consistency checker, which every change to these paths needs.

1. **Flat storage for dense row keys in `oxhaven-column`** (3 to 5 days). *Half done by task
   9.1: `WritableRowRedirection` has it; `WritableSource`, which the aggregation results
   use, does not.* Give
   `WritableRowRedirection` and `WritableSource` a representation backed by a `Vec` for keys
   `0..n`, with previous values tracked only once the table refreshes, instead of a
   `BTreeMap<RowKey, _>` per cell. Sort results, join results over a flat left table, and
   aggregation and `select_distinct` results all have such keys. This one change is under every
   slow case above except `select_distinct`'s own lookup, and it also speeds up every later read
   through a redirection (snapshots, chained operations), which this benchmark does not time.
   Expected: the redirection part of joins and sorts falls from about a tree insertion to an
   array store per row.
2. **Hash the join keys, and keep per-row join state only when refreshing** (3 to 4 days).
   *Done by task 9.1 for `natural_join` and `exact_join`. The same key table
   (`ops/join/key_table.rs`) is there for `join` (`ops/crossjoin`), `aj` (`ops/asof`),
   `multi_join`, `range_join` and `where_in`, which still read a `Vec<Cell>` per row; `join`
   is 1.5x to 2.9x and `aj` 2x to 3x of Java's earlier times.*
   Encode the keys of a chunk with `RowConverter` into one buffer, as aggregation already does,
   look them up in a hash table with a fixed-seed fast hasher (PORTING.md rule 7 allows fixed
   seeds), and build the left rows per slot only for a refreshing left table. With item 1, the
   static `natural_join` and `exact_join` should come within a small factor of Java; today they
   are the worst cases at 18x to 40x.
3. **Sort without a box per row** (2 days). *Done by task 9.3; see "After task 9.3".* Encode the keys once into Arrow `Rows` (one buffer),
   sort row positions with a stable sort comparing borrowed rows, and add a typed path for one
   primitive column (sort `(value, position)` pairs; stability then comes free). With item 1
   this removes both the per-row allocation and the per-row tree insertion. Expected to bring
   `sort_int` from 9x to 10x down to within 2x to 3x of Java.
4. **Grouping: one hash lookup per row, no ordered trees** (2 to 3 days). `select_distinct` should
   use the aggregation engine's `KeyTable` (or the same encoded-row hash table) instead of a
   `BTreeMap<Vec<u8>, _>` and a touched-group map per row; `KeyTable` itself should hash
   borrowed bytes with a fixed-seed fast hasher instead of SipHash and allocate keys in one
   arena. Covers `select_distinct` (8x to 31x) and the high-cardinality aggregations and
   `last_by` (3x to 10x).

After these, the next step is parallelism: Java's 2-thread `where` and `update` are what make the
10M-row filters and formulas 3x to 8x. Splitting a static `where` or `update` into segments over
a thread pool is a separate task, and needs a decision on how the engine schedules work.

## After task 9.2 (2026-11-17): aggregations and `select_distinct`

Task 9.2 (`tasks/9.2-fast-aggregations.md`) replaced the per-row trees of the aggregation
engine: group keys go into an open-addressed hash table with flat key storage
(`ops/group_keys.rs`), result columns are vectors indexed by group (`ops/agg/slot_source.rs`),
`select_distinct` uses the same key table, `first`/`last` of a static or add-only table keep a
row key per group, and `group_by` builds each group's row set once. Results are unchanged.

**How it was measured.** Oxhaven only: the Java numbers are those of run 1 / run 2 above (Java
was not run again). Oxhaven at `2be3c714` (the code of the task's last change), built and run as
above (`cargo build --release --locked -p oxhaven-server`, default options), the cases below
with `bench/bench.py run --cases 'select_distinct_*,last_by,sum_by_*,avg_by_*,count_by_*,agg_by_multi,group_by'`,
twice, 12:23 to 12:26. The load average was 6.3 to 7.4 on the 4 cores during both runs (other
agents were building), higher than in runs 1 and 2 above. Every case's check (row count, column
sums, position-weighted sum, first and last three rows) equals Java's check recorded in
`bench/results/2026-11-17-run1.json`. Raw results: `bench/results/2026-11-17-task-9.2-run*.json`.

Median milliseconds, run 1 / run 2 of each. "ratio" is Oxhaven after 9.2 over Java, run by run.

### 1,000,000 rows

| case | java | oxhaven before | oxhaven after 9.2 | ratio |
| --- | ---: | ---: | ---: | ---: |
| `select_distinct_one` | 16.6 / 13.8 | 323 / 276 | 7.3 / 6.8 | 0.44 / 0.49 |
| `select_distinct_two` | 30.4 / 29.5 | 285 / 249 | 24.4 / 16.6 | 0.80 / 0.56 |
| `last_by` | 20.8 / 23.0 | 207 / 193 | 111 / 61.0 | 5.3 / 2.7 |
| `sum_by_low` | 20.1 / 18.9 | 38.2 / 37.2 | 22.1 / 23.7 | 1.1 / 1.3 |
| `avg_by_low` | 21.2 / 20.1 | 33.5 / 36.8 | 30.6 / 24.5 | 1.4 / 1.2 |
| `count_by_low` | 13.6 / 17.7 | 27.6 / 38.2 | 20.1 / 20.0 | 1.5 / 1.1 |
| `sum_by_med` | 47.9 / 50.5 | 60.6 / 88.5 | 44.3 / 57.6 | 0.92 / 1.1 |
| `avg_by_med` | 53.6 / 60.7 | 81.2 / 91.5 | 67.6 / 86.4 | 1.3 / 1.4 |
| `count_by_med` | 20.6 / 18.3 | 45.8 / 67.3 | 21.0 / 26.1 | 1.0 / 1.4 |
| `sum_by_high` | 183 / 194 | 1126 / 1080 | 261 / 229 | 1.4 / 1.2 |
| `avg_by_high` | 194 / 199 | 1018 / 1161 | 293 / 202 | 1.5 / 1.0 |
| `count_by_high` | 106 / 133 | 807 / 927 | 167 / 148 | 1.6 / 1.1 |
| `agg_by_multi` | 76.2 / 103 | 124 / 130 | 164 / 118 | 2.2 / 1.1 |
| `group_by` | 109 / 79.6 | 366 / 314 | 133 / 98.5 | 1.2 / 1.2 |

### 10,000,000 rows

| case | java | oxhaven before | oxhaven after 9.2 | ratio |
| --- | ---: | ---: | ---: | ---: |
| `select_distinct_one` | 103 / 100 | 3201 / 2767 | 53.5 / 79.2 | 0.52 / 0.79 |
| `select_distinct_two` | 321 / 249 | 2680 / 2385 | 229 / 247 | 0.71 / 0.99 |
| `last_by` | 258 / 221 | 2120 / 1928 | 523 / 594 | 2.0 / 2.7 |
| `sum_by_low` | 166 / 166 | 332 / 288 | 335 / 364 | 2.0 / 2.2 |
| `avg_by_low` | 205 / 158 | 325 / 332 | 315 / 283 | 1.5 / 1.8 |
| `count_by_low` | 144 / 103 | 329 / 226 | 213 / 196 | 1.5 / 1.9 |
| `sum_by_med` | 589 / 379 | 817 / 550 | 517 / 407 | 0.88 / 1.1 |
| `avg_by_med` | 574 / 480 | 892 / 692 | 469 / 567 | 0.82 / 1.2 |
| `count_by_med` | 265 / 150 | 469 / 351 | 232 / 383 | 0.88 / 2.6 |
| `sum_by_high` | 912 / 871 | 3496 / 2813 | 925 / 1123 | 1.0 / 1.3 |
| `avg_by_high` | 1392 / 1021 | 3360 / 2761 | 988 / 1077 | 0.71 / 1.1 |
| `count_by_high` | 498 / 437 | 2691 / 2448 | 524 / 607 | 1.1 / 1.4 |
| `agg_by_multi` | 882 / 743 | 1353 / 1409 | 974 / 1032 | 1.1 / 1.4 |
| `group_by` | 1209 / 934 | 8797 / 6709 | 1220 / 1235 | 1.0 / 1.3 |

**Reading.** `select_distinct` went from 8x-31x slower than Java to faster than Java at both
sizes; the high-cardinality aggregations from 3x-8x to 1x-1.6x; `group_by` from 3x-7x to
1x-1.3x. The low- and medium-cardinality aggregations were within 2x before and are about
the same now, within the noise of a loaded machine. `last_by` improved 2x-4x but is still 2x-5x
slower than Java; its two runs at 1M rows differ by almost 2x, so the load affected it. With
these loads, ratios under about 2x are noise, as above.

The same cases inside the engine, without the server (`cargo run --release -p oxhaven-engine
--example aggregation-cases`, 1M rows, median of 5), before (main at `03f8dbb8`) and after:
`select_distinct_one` 310 → 4.7 ms, `select_distinct_two` 297 → 19.8 ms, `last_by` 166 → 50 ms,
`sum_by_high` 1271 → 261 ms, `count_by_high` 1014 → 170 ms, `group_by` 344 → 155 ms,
`sum_by_low` 31 → 20 ms.

**What is left.** Measured with temporary phase timers in the engine example (1M rows, after
the change): `last_by` spends about 38 ms of its 47 in the operators, one call through a trait
object per group per chunk for each of its six columns; passing a chunk's runs to an operator
in one call would remove most of that, for every aggregation. The 1M-group aggregations spend
90-190 ms finding slots, nearly all for inserting a million new keys into a table that no
longer fits in cache (Java's whole `count_by_high` takes about the same); and 25-60 ms
writing results. `group_by` still gathers every value into Arrow lists at creation, which
Java does not; that is now about 20 ms of its 155 at 1M rows. Not measured here: ticking
throughput (the engine example's 19 append-only cycles of 50,000 rows into `select_distinct`,
`last_by` and `sum_by` went from 4.6 s to 2.4 s; the two aggregators' own cycles were about half of what remains,
and the rest, the input table's writes and `select_distinct`, was not broken down).

## After task 9.4 (2026-11-17): filters and formulas

Task 9.4 (`tasks/9.4-parallel-filter-update.md`) measured where `where` and `update` spend their
time and then changed the formula evaluator and the match and range filters. Results are
unchanged.

**How it was measured.** Inside the engine only, without a server and without Java:
`cargo run --release -p oxhaven-engine --example filter-update-cases -- <rows> <reps> 1,2,4`,
the benchmark's cases on a table built as the benchmark builds its source, median of 5
repetitions at 1M rows and of 3 at 10M. "Before" is `main` at `eceb7e55`, load average 3.0;
"after" is the branch at `6152b690`, load average 3.4 falling to 2.3 (another agent was
building; the machine has 4 cores). The end-to-end benchmark (`bench/bench.py` against a
release server and Java) was **not** run for this task, so the Java column below repeats the
first measurement's medians and is an indication only: the server adds a round trip to every
case. Raw output: `bench/results/2026-11-17-task-9.4-engine-*.txt`.

Median milliseconds. "1 / 2 / 4" are the thread counts (`OXHAVEN_THREADS`).

| case | rows | before (1 thread) | after, 1 / 2 / 4 threads | before / after at 4 | java, first measurement |
| --- | ---: | ---: | ---: | ---: | ---: |
| `where_range` | 1M | 30.9 | 11.1 / 8.8 / 7.6 | 4.1 | 32.0 / 15.0 |
| `where_string` | 1M | 6.1 | 4.5 / 3.1 / 2.2 | 2.8 | 16.6 / 18.3 |
| `where_conj` | 1M | 17.6 | 8.1 / 8.9 / 9.3 | 1.9 | 17.9 / 14.3 |
| `update_arith` | 1M | 73.8 | 24.6 / 10.1 / 8.9 | 8.3 | 43.7 / 31.8 |
| `update_cond` | 1M | 48.0 | 6.1 / 4.8 / 4.2 | 11 | 17.7 / 26.5 |
| `update_string` | 1M | 201 | 112 / 74.9 / 53.7 | 3.7 | 100 / 52.8 |
| `build` | 1M | 357 | 157 / 117 / 75.2 | 4.8 | 173 / 139 |
| `where_range` | 10M | 420 | 216 / 134 / 114 | 3.7 | 52.1 / 48.5 |
| `where_string` | 10M | 64.4 | 50.3 / 37.5 / 19.1 | 3.4 | 121 / 161 |
| `where_conj` | 10M | 258 | 118 / 95.4 / 94.7 | 2.7 | 92.9 / 80.3 |
| `update_arith` | 10M | 960 | 402 / 254 / 202 | 4.8 | 179 / 355 |
| `update_cond` | 10M | 442 | 189 / 112 / 97.4 | 4.5 | 71.9 / 89.7 |
| `update_string` | 10M | 1987 | 1303 / 742 / 393 | 5.1 | 1094 / 1460 |
| `build` | 10M | 4943 | 2147 / 1492 / 1026 | 4.8 | 1738 / 2577 |

**What was slow.** Not, in the first place, the single thread, as the first reading above
guessed. A pass over the `int` column's memory costs 0.4 to 0.9 ns per row on this machine;
evaluating `A * 2 + 1` cost 13 ns per row at 1M rows and 25 at 10M, `A > 50000 ? B : -B` 18 and
46, and the row set of a filter that keeps 40% of the rows 8 to 11. Every operator built its
result from an iterator of `Option`s, copied operands that already had the right type, and at
10M rows worked on intermediates of 40 to 80 MB; the filters widened each cell to an
`Option<i64>` and added kept rows to the row set one key at a time.

**What changed.** Typed loops in the evaluator; formulas made only of operators run in pieces
of 2^16 rows (the size of a segment of Java's parallel `where`) that stay in cache, on as many
threads as `OXHAVEN_THREADS` says (default: the available cores), and are evaluated again in
one pass if a piece fails, so that the failure is the one reported before; match and range
filters have typed kernels and all single-column filters run in the same pieces. Formulas that
call any function keep the old path. On one thread the evaluation alone went, at 10M rows, from
25.1 to 5.0 ns per row for `A * 2 + 1`, from 42.4 to 14.4 for `B * 1.5 - A`, from 46.1 to 16.5
for the conditional and from 32.1 to 5.1 for the range condition.

**What did not get faster, and the next bottlenecks.** `where_conj` gains nothing from threads:
its second and third filters read their columns at scattered rows (4.7 ns per kept row, one
thread) and every filter's row set is built on one thread. `where_range` at 10M rows is still
about 114 ms on 4 threads against Java's 50: the row set of 4M scattered rows (3M ranges) takes
about 120 ms when built one key at a time, and how much of the 114 ms it still is was not
broken down. Formula results with 8-byte values cost three times as much per row at 10M rows as
at 1M (14 against 5 ns for `B * 1.5 - A` on one thread): the pieces are joined by copying them
into one new array. `update_string` and the string column of `build` format one string per row.
Threads scale less than linearly here because the columns are read, the pieces joined and the
row sets built on one thread, and the machine was shared.

## After task 9.3 (2026-11-17): sort, and reads through a row redirection

Task 9.3 (`tasks/9.3-fast-sort.md`) changed how `sort` orders its rows and how a column is
read through a row redirection. This block measures that change only.

**What was compared.** Two Oxhaven servers on the machine above, run by the same runner with
`--cases 'sort_*,natural_join_small,aj,head_half'`, interleaved case by case: "before" is
`main` at `11c29b75`, "after" is the branch of task 9.3 merged with `main` at the time
(task 9.2 included, which touches none of these cases), both built with
`cargo build --release --locked -p oxhaven-server`. Two complete runs, 14:02 to 14:09, with
another agent's builds on the machine (one-minute load 1.3 to 4.7). Every case gave the same
result on both servers in both runs. Java was not run again: the "java (earlier)" column
repeats the medians of the first measurement, taken under another load, so the last column is
an indication and not a measurement of one run. The raw results and the table with min-max and
load per case are in `bench/results/2026-11-17-task-9.3*`.

Median milliseconds of each run, run 1 / run 2.

| case | rows | before | after | before / after | java (earlier) | after / java (earlier) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `sort_int` | 1M | 478 / 489 | 29.5 / 26.0 | 16 / 19 | 116 / 153 | 0.17 to 0.25 |
| `sort_two` | 1M | 443 / 493 | 139 / 126 | 3.2 / 3.9 | 232 / 251 | 0.50 to 0.60 |
| `sort_string` | 1M | 333 / 361 | 114 / 138 | 2.9 / 2.6 | 280 / 317 | 0.36 to 0.49 |
| `natural_join_small` | 1M | 11.4 / 9.4 | 11.6 / 9.9 | 1.0 / 0.9 | | |
| `aj` | 1M | 470 / 482 | 446 / 425 | 1.1 / 1.1 | | |
| `head_half` | 1M | 0.89 / 3.9 | 1.0 / 0.86 | | | |
| `sort_int` | 10M | 10078 / 9740 | 570 / 540 | 18 / 18 | 1175 / 1255 | 0.43 to 0.49 |
| `sort_two` | 10M | 9256 / 9555 | 1728 / 1672 | 5.4 / 5.7 | 2779 / 2810 | 0.60 to 0.62 |
| `sort_string` | 10M | 6398 / 5782 | 1770 / 1615 | 3.6 / 3.6 | 2546 / 2372 | 0.63 to 0.75 |
| `natural_join_small` | 10M | 144 / 158 | 160 / 148 | 0.9 / 1.1 | | |
| `aj` | 10M | 5511 / 5779 | 5411 / 5233 | 1.0 / 1.1 | | |
| `head_half` | 10M | 1.1 / 1.1 | 0.91 / 1.0 | | | |

The join and `head_half` cases did not change beyond the noise.

**What changed.**

- The sort keys of a set of rows are kept in a few flat buffers, never in an allocation per
  row (`ops/sort/keys.rs`). A sort by one column of a fixed-width type (the integers,
  `char`, `boolean`, the floating point types, and the date and time types) maps each value
  to an unsigned integer whose order is Java's (one NaN, the greatest; `-0.0` equal to
  `0.0`; the absolute value when asked), keeps nulls apart, and sorts (value, position) pairs
  with a radix sort by bytes, which skips the bytes every value shares; a stable radix sort
  keeps ties in the parent's order. A sort by one string column keeps the strings' bytes
  in one buffer, with the two lead bytes of U+E000 to U+FFFF moved above those of the
  characters beyond U+FFFF so that byte order is UTF-16 order. Every other sort keeps Arrow's
  row format in one buffer, as before but without copying each row out. Strings and rows are
  sorted as (first 15 bytes and length, position) values, which order exactly whenever a key
  is at most 15 bytes long, and read the whole key only to break a tie of longer ones.
- The result's redirection is made in one piece (`WritableRowRedirection::from_rows`).
- An incremental sort keeps its keys in the same buffers, appended in sorted order each
  cycle, instead of a box per row.
- `RedirectedSource` reads a row set through a redirection a range at a time: for the flat
  array, a copy of the slice for each range of the row set, under one read lock, and the
  mappings changed in the cycle laid over it for previous values. `FlatSource` gathers by a
  plain vector of positions when no key is null.

**Where the time went, and where it goes now.** Measured with
`cargo run --release -p oxhaven-engine --example sort-phases -- 10000000`, which also runs
the parts of the old method with Arrow directly, one run of each build:

| part, 10M rows, sort by the int column | before | after |
| --- | ---: | ---: |
| the row format of the key column, in one buffer | 38 ms | (no row format) |
| copy each row into a box | 371 ms | none |
| stable comparison sort of the boxes | 6081 ms | none |
| the whole sort | 7921 ms | 524 to 560 ms |
| read a column of every row back through the redirection | 2722 ms | 314 to 324 ms |

So the box per row was 5% of the time and the comparison sort itself, which follows a
pointer per comparison, 77%. Sorting positions with a comparison over the one buffer of the
row format instead of the boxes was slower still (9704 ms), which is why the general path
sorts values that carry the start of their key. One redirection write per row cost 236 ms
against 61 ms in one piece. The other shapes, whole sorts before and after at 10M rows: two
columns 6480 ms and 1582 to 1697 ms, a string 4468 ms and 1419 to 1653 ms. At 1M rows the
int sort went from 424 ms to 49 to 60 ms.

**What did not get faster.** An incremental sort of a refreshing table: ten cycles that each
append 1,000 rows to a table of 10M rows took 23642 ms before and 17514 ms after (2056 ms and
1619 ms at 1M rows). The keys are no longer boxed, but each cycle is still linear in the
size of the table: the result is positional, so an insertion shifts every row after it,
and `remap` writes each shifted position into the redirection, which records each write in
an ordered map of previous mappings (`Mappings::prev` in `oxhaven-column/src/redirect.rs`),
10M insertions per cycle here. Reading a column of that result back takes 9940 ms (10402 ms
before), most of it, by reading the code, the input table's column (`WritableSource`), which
is read a cell at a time; that part was not timed apart.

**The next bottlenecks.** For a static sort by two columns or by a string, the comparison
sort of 24-byte values (1.4 to 1.7 s at 10M rows); a radix sort over the row format's bytes
would remove it. For an incremental sort, the previous mappings of the redirection: a
redirection rewritten in bulk could keep its array as of the start of the cycle once (one
copy) instead of one map entry per write, and the result could leave gaps in its key space
as Java's does, so that an insertion moves few rows; the second changes the row keys and
shifts the sort reports.

## After task 9.6 (2026-11-17): incremental sort, and the row set of a filter

Task 9.6 (`tasks/9.6-incremental-sort-rowset-build.md`) took the two bottlenecks the blocks
above end with: the incremental sort of a large table, and the row set of the rows a filter
keeps. Results and row order are unchanged; the row keys of an incremental sort result are
not (`PARITY.md` A4).

**How it was measured.** Inside the engine only, release builds of the two examples, one run
each, on the machine above with another agent building (the load average is given per run):
`sort-phases -- 10000000 --incremental-only` and `filter-update-cases -- 10000000 3 1,4`
(after: `1,2,4`). "Before" is `main` at `1116137f` with the examples extended, "after" the
branch. Java was **not** run again and the end-to-end benchmark (`bench/bench.py`) was not
run: the Java column repeats the medians of the first measurement, taken through a server
and under another load, and is an indication only. Raw output:
`bench/results/2026-11-17-task-9.6-*.txt`.

**Incremental sort.** Ten cycles that each append 1,000 rows to a sorted refreshing table
of 10M rows, milliseconds:

| sort key | before (load 1.7 to 2.4) | after (load 3.9 to 3.5) | one cycle before | one cycle after |
| --- | ---: | ---: | ---: | ---: |
| int | 16815 | 318 | 1503 to 1855 | 29 to 38 |
| string | 17573 | 303 | 1603 to 1902 | 29 to 38 |

The rows read back are the same in the same order (the weighted sums printed by the example
are equal). Making the sorted table took 1100 ms before and 1278 ms after for the int key,
2230 ms and 2416 ms for the string key, under a higher load after; the new layout adds one
pass that places the rows in leaves. Reading a column of every row back took 9620 ms before
and 9786 ms after (int), 4843 ms and 5027 ms (string): unchanged, the time is in the input
table's column. The static sort and its read-back were not measured again; their code did
not change.

Before the change the parts were timed apart: ten million redirection writes take 1032 ms
inside a cycle, where each records the previous mapping in an ordered map, and 156 ms
outside one. So the reading in the block above was right for about 60% of a cycle of
1.7 s; the rest was the other passes over every row.

What changed: the result of an incremental sort keeps free row keys, as Java's does, so an
insertion moves few rows and reports them as shifts (`ops/sort/gapped.rs`). The key space
is cut into leaves of 128 keys, 96 of them used at first; an insertion with no free key
lays out the smallest window of leaves around it that has room. Java starts dense and
leaves a gap of 64 keys after every 64 rows it moves; the keys differ, the rows and their
order do not. What a cycle costs now is the binary searches for the rows that come and go,
the rows of the leaves they touch, and the update of the row set.

**Filters.** Median milliseconds at 10M rows, 3 repetitions; load average 2.1 to 2.4
before, 5.2 to 5.8 after (so the after columns are, if anything, too high):

| case | before, 1 / 4 threads | after, 1 / 2 / 4 threads | java, first measurement |
| --- | ---: | ---: | ---: |
| `where_range` | 180 / 117 | 80.2 / 64.1 / 54.5 | 52.1 / 48.5 |
| `where_range` (two filters) | 313 / 235 | 200 / 105 / 91.8 | |
| `where_string` | 60.4 / 21.7 | 59.9 / 33.5 / 28.6 | 121 / 161 |
| `where_conj` | 117 / 88.8 | 81.6 / 47.2 / 59.1 | 92.9 / 80.3 |

`where_string` did not change in code (few kept rows); its after numbers show the load.

The breakdown before, one thread: `where_range` (180 ms) was 61 ms evaluating the condition,
42 ms finding the 3M kept ranges and about as much again putting them into one builder and
the builder into a row set; on four threads only the first two were divided. In
`where_conj` the first filter took 54 ms, the second 42 ms for 1M rows (20 ms reading the
double column at scattered rows, about 20 ns a row, and 19 ms for the row set) and the
third 22 ms for 650,000 rows, split the same way.

What changed: each piece of 65,536 rows makes its own row set on the thread that judged
it, and the pieces, which are disjoint and in order, are joined by
`oxhaven_rowset::concat_ordered`, which looks at no key again. On one thread a row set per
piece, joined, takes 36 ms where the single builder took 84 ms. A flat column gathers
scattered rows in pieces on several threads (reading the int column at the 4M kept rows:
49 ms on one thread, 28 ms on four).

**What is left.** `where_range` on four threads is about Java's earlier time. Its largest
part is now the evaluation of the condition (51 ms on one thread) and, in a conjunction,
the row set of a piece whose selection is scattered (about 15 ns a row, though on every
thread). For the incremental sort: a parent shift still rewrites every row's parent key,
growing the key space moves every row in one cycle, and reading the result back is slow
for a reason outside the sort (the input table's column is read a cell at a time).

## After task 9.7 (2026-11-17): reads of refreshing tables, formula results, string concatenation

Task 9.7 (`tasks/9.7-refreshing-reads-formula-joins.md`) took four leftovers of the blocks
above. Results, row order and failures are unchanged.

**How it was measured.** Inside the engine only, release builds of the two examples on the
machine above with another agent building (the load average is given per run):
`sort-phases -- 10000000 --reads-only`, `sort-phases -- 10000000 --incremental-only`,
`filter-update-cases -- 10000000 3 1,4` and `filter-update-cases -- 1000000 5 1,4`. "Before"
is `main` at `2cd04f0a` with the examples extended (for the formulas: with the change to
the reads, which they do not use), "after" the branch. Java was **not** run again and the
end-to-end benchmark (`bench/bench.py`) was not run. Raw output:
`bench/results/2026-11-17-task-9.7-*.txt`.

**Reads.** One read of a column at all 10M rows, milliseconds; load average 3.1 falling to
1.9 before, 2.5 to 2.3 after. "Scattered" is a read by keys in an order that is not theirs,
which is what a sort result does through its redirection.

| table, column | in row order, before | after | scattered, before | after |
| --- | ---: | ---: | ---: | ---: |
| static, `Id` (long) | 0.0 | 0.0 | 182 | 55.2 |
| static, `B` (double) | 0.0 | 0.0 | 205 | 48.7 |
| static, `S` (string) | 0.0 | 0.0 | 481 | 105 |
| refreshing, written once, `Id` | 290 | 14.7 | 9408 | 107 |
| refreshing, written once, `B` | 353 | 14.9 | 9347 | 120 |
| refreshing, written once, `S` | 366 | 21.9 | 9487 | 157 |
| refreshing, after ten appends, `Id` | 371 | 57.5 | 9505 | 142 |
| refreshing, after ten appends, `B` | 347 | 57.9 | 9569 | 120 |
| refreshing, after ten appends, `S` | 408 | 26.9 | 10139 | 184 |

A column of a sort of the refreshing table, read back at every row: 9102 ms before, 244 ms
after; in the incremental run of the block above (`--incremental-only`, load 3.2) 232 ms
for the int key and 198 ms for the string key, where task 9.6 measured 9786 ms and
5027 ms. The weighted sums are the same.

The cause was not the sort and not a lock or the previous values: a refreshing table's
column (`WritableSource`) kept the place of each row key's value in an ordered map, so a
read by key was a map lookup per row, 0.9 microseconds each in a map of 10M entries, and a
read in row order walked the map. The places are now in a flat array indexed by row key
while the keys are dense, as the row redirection's mappings already were, and in the map
otherwise. Rows that were written in row order are read as a slice of the written array
(or a concatenation of a few slices, the 57 ms above); other rows are gathered in pieces
of 65,536 rows on several threads. A static column read by keys is gathered in the same
pieces, where it was one gather on one thread. Every table whose columns are written by
the engine reads this way: input tables, ring tables, the tables Kafka feeds, and the
columns `update` and `select` make on a refreshing table. What is left of the difference
from a static column in row order is one pass over the 10M places to see that they follow
each other.

**Formulas.** Median milliseconds at 10M rows, 3 repetitions; load average 2.3 to 2.1
before, 3.3 to 3.1 after (so the after columns are, if anything, too high).

| case | before, 1 / 4 threads | after, 1 / 4 threads |
| --- | ---: | ---: |
| `update_arith` | 255 / 173 | 229 / 132 |
| `update_cond` | 120 / 94.9 | 96.9 / 53.9 |
| `update_string` | 1274 / 496 | 236 / 112 |
| `build` | 2096 / 1087 | 1000 / 545 |
| `build` (numbers only) | 633 / 443 | 714 / 413 |

| part, one thread | before | after |
| --- | ---: | ---: |
| evaluate `A * 2 + 1` | 40.9 | 37.3 |
| evaluate `B * 1.5 - A` | 105 | 96.1 |
| evaluate `Id % 7` | 104 | 99.7 |
| evaluate `A > 50000 ? B : -B` | 134 | 115 |
| evaluate ``S + `_x` `` | 1534 | 245 |
| evaluate `` `s` + (Id % 1000) `` | 1537 | 305 |

At 1M rows after the change (5 repetitions, load 3.1; no run before it): `update_arith`
19.9 / 6.1, `update_cond` 6.5 / 3.3, `update_string` 23.1 / 10.2, `build` 67.8 / 36.0.

*String concatenation* made three `String`s per row: each side as text, then the two
joined. The result's bytes and offsets are now written directly, strings, ints and longs
straight from their arrays. That is the six-fold gain of `update_string` on one thread and
most of `build`'s.

*The join of a formula's pieces.* The pieces of a result of fixed-width values are no
longer joined by a copy on one thread after all are computed: each piece copies its values
into its own part of one buffer, on the thread that computed it
(`oxhaven_column::parallel::fill_pieces`, which hands out the parts with `chunks_mut`). On
four threads that is the gain of `update_arith` and `update_cond`. On one thread the gain
is small, and the reading of the block of task 9.4 ("8-byte results cost three times as
much per row at 10M rows as at 1M because the pieces are joined by a copy") was only
partly right. Measured apart at 10M rows: joining 153 pieces of doubles with Arrow's
`concat` takes 40 to 49 ms, copying them into one buffer made beforehand takes the same,
and computing `B * 1.5` straight into such a buffer with no pieces at all takes 33 to
39 ms, against 1.1 ms for 1M rows. The cost is writing 80 MB of memory for the first time
(the kernel hands out and clears each page), not the copy; at 1M rows the repetitions of
the example get the memory of the repetition before back from the allocator, so they do
not pay it. A new result of 80 MB pays it once, whatever writes it; the change removes the
second time (the pieces' own memory is now given back and used again) and moves the first
onto all threads.

**What is left.** Formulas: the pieces of a string result are still joined by a copy on
one thread (about 50 ms of `update_string`'s 112 ms on four threads, by the size of the
result; not timed apart), and the other operand types of a concatenation (floating point,
`boolean`, `char`, the date and time types) still make a `String` per row first. The
first write of a large result could only be avoided by not making the result (a `view`),
or made cheaper by huge pages. Reads: a scattered read of a refreshing column costs two
random memory reads a row (the place, then the value) where a static column costs one,
120 to 180 ms against 50 to 100 ms on four threads; a previous-value read by keys looks
each key up in the map of the cycle's changes when the cycle changed anything.

## After task 9.5 (2026-11-17): one key table, the keyed cross join, `last_by` and `group_by`

Task 9.5 (`tasks/9.5-key-table-followups.md`) took the follow-ups that 9.1 and 9.2 left: one
hash table for every operation that numbers keys, the keyed cross join on it, `last_by`
without a call through a trait object per group, and `group_by` without copying every value
twice. Results, row order and incremental behaviour are unchanged.

**How it was measured.** Inside the engine only, release builds of the examples, on the
machine above with another agent building (the load average is given per run). Java was
**not** run again and the end-to-end benchmark (`bench/bench.py`) was not run. Raw output:
`bench/results/2026-11-17-task-9.5-*.txt`; the key table numbers are in the task file.

**The key table** (`examples/key-table-cases.rs`, 1M rows, median of 7, milliseconds, load
1.9 to 2.3, so differences under about 20% are noise). "join" and "group" are the two
tables it replaced; each cell is new keys inserted / the same keys found again, in chunks
of 4,096 rows as an aggregation asks:

| keys | join | group | one table |
| --- | ---: | ---: | ---: |
| int, 10 distinct | 3.1 / 3.0 | 2.3 / 2.1 | 1.7 / 1.7 |
| int, 1M distinct | 63.8 / 28.8 | 68.7 / 20.4 | 62.2 / 12.4 |
| double, 1M distinct | 66.6 / 25.3 | 70.2 / 25.5 | 66.0 / 15.5 |
| string, 1M distinct | 163.0 / 46.9 | 118.5 / 60.6 | 88.4 / 41.8 |
| int and string, 1,000 distinct | 18.5 / 23.8 | 24.2 / 18.5 | 14.7 / 14.9 |

In `examples/aggregation-cases.rs` that change took `select_distinct_two` from 20.4 to
14.8 ms, `last_by` from 56.7 to 39.5 and `sum_by_low` from 19.1 to 15.4 (medians; the task
file has every case). Inserting a million new keys did not get faster (60 to 80 ms either
way); it is the largest part left of `natural_join_large` and of the aggregations by a key
of 1M values.

**The keyed cross join** (`examples/crossjoin-cases.rs`, 1M left rows, median (min) of 7,
milliseconds; load 1.8 before, 2.9 after). The first case is the shape of `join_on_key`:

| case | before | after |
| --- | ---: | ---: |
| int key, 10,000 right rows, one per key | 562 (483) | 17.2 (15.3) |
| then reading the added column of every row | 187 (152) | 27.4 (18.7) |
| int key, 1,000,000 right rows, one per key | 3368 (3222) | 168 (143) |
| then reading the added column of every row | 357 (328) | 22.7 (21.6) |
| int key, 1,000 keys, 3 right rows per key | 473 (425) | 9.8 (9.6) |
| string key, 1,000 right rows, one per key | 451 (389) | 25.9 (24.1) |
| refreshing left, static right, 10,000 right rows | 618 (494) | 41.7 (40.3) |
| then 20 cycles that append 1,000 left rows each | 90.1 (66.4) | 50.1 (48.9) |
| static left, refreshing right | 605 (547) | 54.5 (52.1) |

Timed in parts before the change, the join of the first case spent about 250 ms making a
`Vec` key per row and numbering it in an ordered map, 100 to 250 ms putting the slot of
every left row in a second ordered map, and the rest grouping rows by slot in a third; a
read of the added column looked each row up in two of them. Keys are now numbered a chunk
at a time by the one key table, the slot of each left row is a row redirection (a flat
array for dense row keys), and the right rows of a static right table are one array
grouped by slot. In the case of 1M right keys the rest is mostly the key table inserting
1M keys and finding 1M (about 80 ms).

**Aggregations** (`examples/aggregation-cases.rs 1000000 9`, two interleaved rounds of
before and after, median (min), milliseconds; load 5.2 to 5.4 throughout, so only the
differences that hold in both rounds count):

| case | before, round 1 | after, round 1 | before, round 2 | after, round 2 |
| --- | ---: | ---: | ---: | ---: |
| `last_by` | 47.9 (45.1) | 40.8 (37.8) | 46.5 (45.0) | 37.8 (34.4) |
| `group_by` | 81.1 (74.2) | 63.0 (56.6) | 74.4 (61.8) | 71.5 (58.1) |
| `group_by_read` | 81.7 (71.7) | 56.3 (55.8) | 78.6 (75.9) | 61.0 (56.2) |
| `agg_by_multi` | 88.7 (70.3) | 84.1 (81.8) | 66.9 (64.9) | 85.5 (82.7) |
| `count_by_low` | 15.1 (14.8) | 15.2 (14.9) | 11.6 (11.5) | 15.1 (14.9) |
| `append_only_cycles` | 2590 | 2390 | 2500 | 2044 |

The `group_by` numbers above 9.2 and 9.5's item 1 (139 and 137 ms) included building the
1M-row table inside the timing, which was most of them; the example now builds it first,
and `group_by_read` also reads every vector back. `agg_by_multi` and `count_by_low` vary
between rounds and runs with no change in their code path; under `valgrind --tool=callgrind`
the branch runs 3.4% fewer instructions for `agg_by_multi` and the same number for
`count_by_low`, so their differences here are noise or code layout.

- `last_by` gives an operator every group of a chunk in one call (`AggOperator::add_runs`),
  which first and last take without making a `Run` per group; the instructions of the
  aggregation's apply step fell 28%. The operator's own work per group (a row key compared
  and stored at a random slot) is unchanged, and is now most of it.
- `group_by` used to gather each column twice: once in group order to read the groups, then
  again with `take` to make the result. The result is now the first gather itself when the
  groups are asked for in the order they were read, which is how the framework asks; the
  result step went from 209M to 8M instructions. What is left, per call: about 260M
  instructions in the work every aggregation does (numbering keys, grouping a chunk), 195M
  making each group's row set, and 125M reading the groups' values back through those row
  sets. Making the vectors lazy, as Java's `AggregateColumnSource` does, would move the last
  part to the reader rather than remove it, and every reader of the benchmark reads every
  vector, so it was not done.

## Second measurement (after tasks 9.1 to 9.7)

Task 9.8 (`tasks/9.8-benchmark-rerun.md`) ran the whole benchmark again, Deephaven and
Oxhaven, once tasks 9.1 to 9.7 were merged. It is the first end-to-end run with Deephaven
since the first measurement; the blocks of tasks 9.4 to 9.7 measured inside the engine only.

The short reading: every case gave the same result on both servers in all three runs, and
none failed. Of the 74 pairs of case and size, Oxhaven is now faster than Deephaven in 34
(ratio below 0.8 in both runs), level or between level and slower in 31, and slower by
more than 20% in both runs in 9: `aj` (2x at 1M rows, 3x at 10M), `last_by` (2x to 2.7x), `ungroup` at 10M rows (a few
milliseconds), `where_range` at 10M rows, and three low- and medium-cardinality
aggregations at 10M rows (1.2x to 1.5x).

**How it was measured.** As the first measurement (see "The machine and the servers"),
with these differences:

- **Oxhaven:** `main` at `8a21018b` (which contains 9.1 to 9.7, the last merged as
  `55bb54de`), built with `cargo build --release --locked -p oxhaven-server`, default
  release profile, without the `python` feature, run with default options. The engine now
  has parallel code (tasks 9.4, 9.6, 9.7): by default a table operation may use 4 threads
  on this machine (the server logs "Table operations use up to 4 threads"). A third run
  restarted the same binary with `--threads 1`.
- **Deephaven:** unchanged: 42.9 from `oracle/requirements.txt`, started with
  `oracle/bin/java_server.py --port 10868 --psk oxhaven --heap 5g`, so
  `-XX:ActiveProcessorCount=2` and a 5 GB heap as in the first measurement; OpenJDK
  21.0.12.1. One process for all three runs.
- **Both servers and the client shared the machine's 4 cores.** Deephaven is held to 2
  processors by its JVM setting; Oxhaven's default uses all 4. That is part of the
  comparison, not corrected for; the one-thread column shows Oxhaven without it.
  [Third measurement](#third-measurement-deephaven-on-four-processors-and-both-on-one-thread) corrects for it: both on 4, and both on one.
- **Load: low this time.** No other agent was building. Run 1 started when the one-minute
  load average was 0.87; it was 1.21 at the end of run 1, 1.29 at the end of run 2, and 1.03
  at the end of run 3. The load recorded before each case was 0.9 to 2.0, all of it the
  benchmark itself. The first measurement ran at a load of 2.9 to 9.2, so its ratios in the
  "first" column were taken under very different conditions.
- **Runs:** run 1 17:31:59 to 17:36:21, run 2 17:36:21 to 17:40:30, run 3 (`--threads 1`)
  17:40:36 to 17:44:50; default repetitions (2 warm-up and 5 timed at 1M rows, 1 and 3 at
  10M). A whole run now takes about 4 minutes, where the first took 14 to 15.
- **Two new cases** (see the table of cases): `sort_refreshing_append` and
  `doget_sorted_refreshing`. The other cases the performance tasks measured only inside the
  engine were already in `bench.py`: the keyed cross join is `join_on_key`, and `group_by`
  is `group_by`.

Raw results: `bench/results/2026-11-17-task-9.8-run1.json`, `-run2.json` and
`-threads1.json`; the table with min-max and load per case of every run is
`bench/results/2026-11-17-task-9.8.md`.

Median milliseconds, run 1 / run 2. "Oxhaven, 1 thread" is run 3. "ratio" is Oxhaven over
Deephaven per run; "ratio, first measurement" is the same ratio from the first measurement's
runs 1 / 2 above. "same result" is runs 1 / 2 / 3.

### 1,000,000 rows

| case | Deephaven | Oxhaven | Oxhaven, 1 thread | ratio | ratio, first measurement | same result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `build` | 203 / 77.2 | 42.0 / 27.8 | 71.9 | 0.21 / 0.36 | 1.90 / 2.87 | yes / yes / yes |
| `update_arith` | 37.9 / 23.7 | 8.0 / 9.3 | 12.0 | 0.21 / 0.39 | 1.26 / 2.31 | yes / yes / yes |
| `update_string` | 71.5 / 32.0 | 10.6 / 9.4 | 16.2 | 0.15 / 0.29 | 1.72 / 3.96 | yes / yes / yes |
| `update_cond` | 17.5 / 16.2 | 3.5 / 4.1 | 7.4 | 0.20 / 0.25 | 1.39 / 0.95 | yes / yes / yes |
| `where_range` | 15.6 / 10.1 | 4.6 / 5.9 | 9.3 | 0.29 / 0.59 | 0.90 / 2.81 | yes / yes / yes |
| `where_string` | 16.9 / 15.0 | 3.6 / 2.7 | 6.6 | 0.22 / 0.18 | 0.54 / 0.52 | yes / yes / yes |
| `where_conj` | 18.8 / 12.1 | 8.3 / 9.0 | 8.7 | 0.44 / 0.74 | 1.22 / 1.70 | yes / yes / yes |
| `sort_int` | 105 / 136 | 45.9 / 23.5 | 64.5 | 0.44 / 0.17 | 5.25 / 4.04 | yes / yes / yes |
| `sort_two` | 223 / 204 | 125 / 181 | 146 | 0.56 / 0.89 | 2.28 / 2.48 | yes / yes / yes |
| `sort_string` | 256 / 231 | 118 / 109 | 106 | 0.46 / 0.47 | 1.48 / 1.42 | yes / yes / yes |
| `sum_by_low` | 19.7 / 17.9 | 21.4 / 22.9 | 23.5 | 1.08 / 1.28 | 1.89 / 1.97 | yes / yes / yes |
| `avg_by_low` | 28.6 / 18.1 | 35.0 / 20.8 | 20.9 | 1.23 / 1.15 | 1.58 / 1.83 | yes / yes / yes |
| `count_by_low` | 22.6 / 12.7 | 18.2 / 25.8 | 17.7 | 0.80 / 2.03 | 2.03 / 2.16 | yes / yes / yes |
| `sum_by_med` | 39.3 / 41.2 | 56.8 / 41.6 | 49.9 | 1.44 / 1.01 | 1.26 / 1.75 | yes / yes / yes |
| `avg_by_med` | 42.0 / 55.1 | 47.2 / 46.0 | 49.2 | 1.13 / 0.83 | 1.52 / 1.51 | yes / yes / yes |
| `count_by_med` | 26.4 / 19.5 | 20.6 / 23.1 | 20.5 | 0.78 / 1.18 | 2.22 / 3.68 | yes / yes / yes |
| `sum_by_high` | 179 / 160 | 215 / 170 | 190 | 1.20 / 1.06 | 6.17 / 5.58 | yes / yes / yes |
| `avg_by_high` | 140 / 170 | 237 / 181 | 212 | 1.69 / 1.06 | 5.25 / 5.83 | yes / yes / yes |
| `count_by_high` | 114 / 114 | 119 / 111 | 112 | 1.04 / 0.97 | 7.62 / 6.98 | yes / yes / yes |
| `agg_by_multi` | 67.8 / 80.1 | 89.8 / 87.3 | 68.3 | 1.32 / 1.09 | 1.62 / 1.27 | yes / yes / yes |
| `last_by` | 18.2 / 20.1 | 39.4 / 39.6 | 38.3 | 2.16 / 1.97 | 9.95 / 8.36 | yes / yes / yes |
| `natural_join_small` | 22.6 / 23.8 | 9.9 / 9.9 | 15.1 | 0.44 / 0.42 | 22.82 / 27.58 | yes / yes / yes |
| `natural_join_large` | 94.6 / 102 | 96.6 / 94.7 | 95.1 | 1.02 / 0.93 | 18.57 / 20.59 | yes / yes / yes |
| `exact_join_small` | 29.3 / 36.2 | 10.3 / 9.5 | 8.1 | 0.35 / 0.26 | 26.30 / 27.86 | yes / yes / yes |
| `exact_join_large` | 106 / 96.9 | 94.4 / 97.7 | 104 | 0.89 / 1.01 | 18.45 / 19.92 | yes / yes / yes |
| `join_on_key` | 258 / 239 | 22.5 / 32.8 | 22.2 | 0.09 / 0.14 | 1.63 / 1.87 | yes / yes / yes |
| `aj` | 212 / 193 | 412 / 392 | 443 | 1.94 / 2.03 | 2.82 / 2.89 | yes / yes / yes |
| `select_distinct_one` | 12.7 / 13.1 | 7.5 / 6.8 | 6.3 | 0.59 / 0.52 | 19.50 / 19.96 | yes / yes / yes |
| `select_distinct_two` | 27.2 / 26.4 | 24.1 / 22.0 | 22.0 | 0.89 / 0.83 | 9.36 / 8.44 | yes / yes / yes |
| `head_half` | 2.1 / 2.1 | 0.84 / 1.1 | 0.96 | 0.39 / 0.52 | 0.43 / 0.51 | yes / yes / yes |
| `tail_half` | 2.1 / 2.1 | 0.86 / 1.0 | 0.81 | 0.40 / 0.49 | 0.47 / 0.54 | yes / yes / yes |
| `group_by` | 80.3 / 82.9 | 76.8 / 95.1 | 79.6 | 0.96 / 1.15 | 3.35 / 3.94 | yes / yes / yes |
| `ungroup` | 5.4 / 3.0 | 1.8 / 2.2 | 2.0 | 0.33 / 0.73 | 0.34 / 1.54 | yes / yes / yes |
| `doget_1m` | 68.4 / 64.5 | 27.2 / 29.4 | 31.6 | 0.40 / 0.46 | 0.40 / 0.46 | yes / yes / yes |
| `sort_refreshing_append` | 33.0 / 21.1 | 30.4 / 31.1 | 33.2 | 0.92 / 1.47 | new case | yes / yes / yes |
| `doget_sorted_refreshing` | 139 / 117 | 43.6 / 57.0 | 89.6 | 0.31 / 0.49 | new case | yes / yes / yes |
| `ticking_append_where_sum_by` | 50.1 / 37.9 | 35.2 / 40.2 | 36.6 | 0.70 / 1.06 | 1.70 / 1.76 | yes / yes / yes |

### 10,000,000 rows

| case | Deephaven | Oxhaven | Oxhaven, 1 thread | ratio | ratio, first measurement | same result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `build` | 1393 / 1305 | 641 / 527 | 995 | 0.46 / 0.40 | 3.11 / 2.09 | yes / yes / yes |
| `update_arith` | 129 / 144 | 103 / 108 | 195 | 0.80 / 0.75 | 5.87 / 2.80 | yes / yes / yes |
| `update_string` | 782 / 691 | 160 / 85.3 | 201 | 0.21 / 0.12 | 1.96 / 1.47 | yes / yes / yes |
| `update_cond` | 83.6 / 69.7 | 62.1 / 66.7 | 118 | 0.74 / 0.96 | 5.46 / 4.47 | yes / yes / yes |
| `where_range` | 37.0 / 31.3 | 51.8 / 56.3 | 87.7 | 1.40 / 1.80 | 8.22 / 8.29 | yes / yes / yes |
| `where_string` | 52.0 / 73.4 | 29.3 / 29.6 | 54.2 | 0.56 / 0.40 | 0.68 / 0.60 | yes / yes / yes |
| `where_conj` | 51.4 / 51.9 | 47.7 / 55.6 | 97.4 | 0.93 / 1.07 | 2.95 / 3.33 | yes / yes / yes |
| `sort_int` | 1558 / 1360 | 519 / 527 | 527 | 0.33 / 0.39 | 10.02 / 9.00 | yes / yes / yes |
| `sort_two` | 2468 / 2509 | 1737 / 1609 | 1571 | 0.70 / 0.64 | 4.21 / 3.99 | yes / yes / yes |
| `sort_string` | 2134 / 2039 | 1308 / 1538 | 1422 | 0.61 / 0.75 | 2.69 / 2.80 | yes / yes / yes |
| `sum_by_low` | 152 / 177 | 208 / 198 | 195 | 1.36 / 1.12 | 2.00 / 1.73 | yes / yes / yes |
| `avg_by_low` | 156 / 140 | 212 / 216 | 200 | 1.36 / 1.54 | 1.59 / 2.11 | yes / yes / yes |
| `count_by_low` | 106 / 107 | 158 / 154 | 155 | 1.49 / 1.43 | 2.29 / 2.18 | yes / yes / yes |
| `sum_by_med` | 344 / 349 | 418 / 433 | 427 | 1.22 / 1.24 | 1.39 / 1.45 | yes / yes / yes |
| `avg_by_med` | 436 / 522 | 517 / 497 | 415 | 1.19 / 0.95 | 1.55 / 1.44 | yes / yes / yes |
| `count_by_med` | 151 / 193 | 178 / 180 | 159 | 1.17 / 0.93 | 1.77 / 2.35 | yes / yes / yes |
| `sum_by_high` | 768 / 754 | 723 / 691 | 660 | 0.94 / 0.92 | 3.84 / 3.23 | yes / yes / yes |
| `avg_by_high` | 967 / 885 | 715 / 821 | 700 | 0.74 / 0.93 | 2.41 / 2.70 | yes / yes / yes |
| `count_by_high` | 479 / 499 | 396 / 429 | 369 | 0.83 / 0.86 | 5.40 / 5.60 | yes / yes / yes |
| `agg_by_multi` | 619 / 774 | 1068 / 818 | 847 | 1.73 / 1.06 | 1.53 / 1.89 | yes / yes / yes |
| `last_by` | 160 / 183 | 423 / 373 | 372 | 2.65 / 2.04 | 8.20 / 8.73 | yes / yes / yes |
| `natural_join_small` | 233 / 247 | 122 / 133 | 116 | 0.52 / 0.54 | 27.62 / 28.70 | yes / yes / yes |
| `natural_join_large` | 472 / 439 | 508 / 414 | 375 | 1.08 / 0.94 | 34.90 / 33.84 | yes / yes / yes |
| `exact_join_small` | 224 / 229 | 136 / 120 | 121 | 0.61 / 0.53 | 40.39 / 37.30 | yes / yes / yes |
| `exact_join_large` | 474 / 488 | 406 / 468 | 421 | 0.86 / 0.96 | 29.56 / 33.39 | yes / yes / yes |
| `join_on_key` | 2284 / 2197 | 340 / 341 | 361 | 0.15 / 0.16 | 1.89 / 2.70 | yes / yes / yes |
| `aj` | 1776 / 1689 | 5464 / 4950 | 5636 | 3.08 / 2.93 | 2.45 / 3.14 | yes / yes / yes |
| `select_distinct_one` | 128 / 88.9 | 51.3 / 60.3 | 46.3 | 0.40 / 0.68 | 31.15 / 27.58 | yes / yes / yes |
| `select_distinct_two` | 239 / 228 | 227 / 205 | 163 | 0.95 / 0.90 | 8.34 / 9.56 | yes / yes / yes |
| `head_half` | 2.4 / 1.9 | 1.3 / 1.0 | 0.97 | 0.54 / 0.55 | 0.32 / 1.10 | yes / yes / yes |
| `tail_half` | 2.1 / 1.8 | 1.0 / 1.2 | 0.76 | 0.48 / 0.67 | 2.40 / 2.88 | yes / yes / yes |
| `group_by` | 838 / 942 | 872 / 845 | 983 | 1.04 / 0.90 | 7.28 / 7.19 | yes / yes / yes |
| `ungroup` | 3.9 / 2.9 | 9.3 / 9.1 | 8.6 | 2.38 / 3.10 | 6.15 / 4.06 | yes / yes / yes |
| `doget_1m` | 75.6 / 53.2 | 29.8 / 28.9 | 32.5 | 0.39 / 0.54 | 0.44 / 0.63 | yes / yes / yes |
| `sort_refreshing_append` | 785 / 956 | 36.1 / 39.7 | 29.9 | 0.05 / 0.04 | new case | yes / yes / yes |
| `doget_sorted_refreshing` | 131 / 168 | 63.3 / 66.2 | 173 | 0.48 / 0.39 | new case | yes / yes / yes |
| `ticking_append_where_sum_by` | 98.2 / 85.0 | 36.3 / 38.2 | 38.0 | 0.37 / 0.45 | 2.21 / 2.44 | yes / yes / yes |


**Where Oxhaven is now faster** (ratio below 0.8 in both runs): every formula case at 1M
rows (`build`, `update_arith`, `update_string`, `update_cond`: 0.15 to 0.39) and `build`
and `update_string` at 10M; every filter at 1M rows, and `where_string` at both sizes;
every static sort at both sizes (`sort_int` 0.17 to 0.44, `sort_two` and `sort_string`
0.46 to 0.75, except `sort_two` at 1M in run 2, 0.89); `natural_join_small` and
`exact_join_small` (0.26 to 0.61); `join_on_key` (0.09 to 0.16: 22 to 33 ms against
Deephaven's 239 to 258 ms at 1M rows, 340 ms against 2.2 s at 10M); `select_distinct_one`;
`head_half` and `tail_half` at both sizes and `ungroup` at 1M rows; the Arrow Flight reads
`doget_1m` and `doget_sorted_refreshing` (0.31 to 0.54); the ticking case at 10M rows (0.37
to 0.45); and `sort_refreshing_append` at 10M rows (0.04 to 0.05: 36 to 40 ms against
Deephaven's 785 to 956 ms; why Deephaven takes that long here was not looked into).

**Where it is level** (within about 20% in both runs): `natural_join_large` and
`exact_join_large` at both sizes; `group_by` at both sizes; `select_distinct_two` at both
sizes; `sum_by_high` and `count_by_high` at 1M rows, and all three high-cardinality
aggregations at 10M (0.74 to 0.94, so level to slightly faster); `avg_by_med` and
`count_by_med` at both sizes; `update_arith` (0.80 / 0.75), `update_cond` and `where_conj`
at 10M rows; the ticking case at 1M rows.

**Between level and slower**, where one run was above 1.2 and the other within 20%, so the
runs do not agree: `agg_by_multi` (1.32 / 1.09 at 1M, 1.73 / 1.06 at 10M), `count_by_low`
at 1M (0.80 / 2.03), `avg_by_high` at 1M (1.69 / 1.06), `sort_refreshing_append` at 1M
(0.92 / 1.47), `sum_by_med` at 1M (1.44 / 1.01), `sum_by_low` (1.08 / 1.28 at 1M,
1.12 / 1.36 at 10M) and `avg_by_low` at 1M (1.15 / 1.23). Task 9.5 found `agg_by_multi` and
`count_by_low` varying between runs with no change in their code path.

**Still slower than Deephaven by more than 20% in both runs**, by ratio, largest first:

| case | rows | ratio | likely cause |
| --- | ---: | ---: | --- |
| `aj` | 10M | 3.08 / 2.93 | `ops/asof` still reads a `Vec<Cell>` key per row and keeps its buckets in a `BTreeMap` (the "After task 9.1" block and follow-up 2); no task has changed it |
| `ungroup` | 10M | 2.38 / 3.10 | cause not known (9.3 / 9.1 ms against 3.9 / 2.9 ms; the first reading put it down to round trips at this size, but it held in all three runs) |
| `last_by` | 10M | 2.65 / 2.04 | the operator's own work per group, a row key compared and stored at a random slot (the "After task 9.5" block) |
| `last_by` | 1M | 2.16 / 1.97 | as above |
| `aj` | 1M | 1.94 / 2.03 | as `aj` at 10M |
| `where_range` | 10M | 1.40 / 1.80 | the evaluation of the condition and the row set of the kept rows (the "After task 9.6" block); 52 to 56 ms against 31 to 37 ms |
| `count_by_low` | 10M | 1.49 / 1.43 | cause not known |
| `avg_by_low` | 10M | 1.36 / 1.54 | cause not known |
| `sum_by_med` | 10M | 1.22 / 1.24 | cause not known |

**What the threads contribute** (run 3 against runs 1 and 2). The operations with parallel
code are 1.3x to 2.8x faster on the default 4 threads. At 10M rows, one thread against runs
1 / 2: `update_arith` 195 ms against 103 / 108, `update_cond` 118 against 62 / 67,
`where_range` 88 against 52 / 56, `where_conj` 97 against 48 / 56, `where_string` 54
against 29 / 30, `build` 995 against 641 / 527, `update_string` 201 against 160 / 85, and
`doget_sorted_refreshing` 173 against 63 / 66 (a scattered read of a refreshing column is
gathered on several threads). On one thread, at 10M rows, `where_range` is 2.4x to 2.8x
Deephaven's time, `where_conj` 1.9x, `update_cond` 1.4x to 1.7x and `update_arith` 1.4x to
1.5x; `build`, `update_string` and `where_string` stay faster or level, and at 1M rows
every formula and filter stays faster or level on one thread. Most other cases differ from
runs 1 and 2 by no more than those two runs differ from each other, in both directions;
the larger differences are `natural_join_small` at 1M rows (15.1 ms on one thread against
9.9 / 9.9), `sort_int` at 1M (64.5 against 45.9 / 23.5), and, lower on one thread,
`agg_by_multi` at 1M (68.3 against 89.8 / 87.3) and `select_distinct_two` at 10M (163
against 227 / 205). Whether those use threads was not checked.

**Against the first measurement.** The ratio fell for nearly every case. It did not clearly
fall for `aj` at 10M rows (2.45 / 3.14 then, 3.08 / 2.93 now), `agg_by_multi` at 10M
(1.53 / 1.89 then, 1.73 / 1.06 now), `sum_by_med` at 1M (1.26 / 1.75 then, 1.44 / 1.01
now), `head_half` at 10M (0.32 / 1.10 then, 0.54 / 0.55 now) and `doget_1m` (0.40 to 0.63
then and now). The first measurement ran under a load of 3 to 9 and this one under 1 to 2;
Deephaven's own medians are mostly lower now too (for example `join_on_key` at 1M rows,
365 / 314 ms then, 258 / 239 now), so part of the change of a ratio is the load.

## After task 9.10 (2026-11-17): filters and operator formulas on one thread

Task 9.10 (`tasks/9.10-single-thread-filter-formula.md`) took the four cases in which the
second measurement shows Oxhaven on one thread slower than Deephaven at 10M rows:
`where_range`, `where_conj`, `update_arith` and `update_cond`. Results, row order and
failures are unchanged. The task stopped before it was finished: `where_conj` is not where
it should be, and the task file's Handover says what is left.

**How it was measured.** Inside the engine only, a release build of
`filter-update-cases` on the machine above while another agent was building:
`filter-update-cases -- 10000000 3 1,4` and `-- 1000000 5 1,4`. "Before" is `main` at
`8a21018b`, "after" the branch; the two binaries were run one after the other. Load
average: 2.2 for both runs at 1M rows, 1.6 to 1.9 before and 1.9 to 2.1 after at 10M.
Deephaven was **not** run again and the end-to-end benchmark (`bench/bench.py`) was not
run: the Deephaven column repeats the second measurement's medians, taken through a server,
and is an indication only. Raw output: `bench/results/2026-11-17-task-9.10-engine-*.txt`.

Median milliseconds, before / after.

| case | rows | 1 thread | 4 threads | Deephaven, second measurement |
| --- | ---: | ---: | ---: | ---: |
| `where_range` | 1M | 8.7 / 3.6 | 3.5 / 2.8 | 15.6 / 10.1 |
| `where_conj` | 1M | 9.8 / 8.5 | 6.8 / 7.1 | 18.8 / 12.1 |
| `update_arith` | 1M | 18.7 / 12.3 | 6.2 / 11.1 | 37.9 / 23.7 |
| `update_cond` | 1M | 6.4 / 2.8 | 3.2 / 2.9 | 17.5 / 16.2 |
| `where_range` | 10M | 73.9 / 40.9 | 39.7 / 22.9 | 37.0 / 31.3 |
| `where_conj` | 10M | 98.4 / 74.7 | 31.0 / 27.6 | 51.4 / 51.9 |
| `update_arith` | 10M | 239 / 161 | 92.5 / 102 | 129 / 144 |
| `update_cond` | 10M | 115 / 75.0 | 46.9 / 48.7 | 83.6 / 69.7 |

The four-thread times of the formulas moved within the noise of a shared machine:
`update_arith` at 1M rows on four threads, run again twice with seven repetitions, each
binary straight after the other, gave 10.1 and 9.3 ms before and 7.1 and 7.6 ms after
(`update_cond` 3.4 and 3.1 before, 1.8 and 1.9 after), not the 6.2 / 11.1 of the table.

| part, one thread, 10M rows | before | after |
| --- | ---: | ---: |
| evaluate `A >= 20000 && A < 60000` | 54.8 | 6.3 |
| evaluate `A * 2 + 1` | 35.0 | 22.7 |
| evaluate `B * 1.5 - A` | 86.9 | 70.3 |
| evaluate `Id % 7` | 95.2 | 73.2 |
| evaluate `A > 50000 ? B : -B` | 115 | 104 (min 75.3) |
| a row set per piece from the mask, joined | 34.0 | 21.1 |

**What was slow.** A comparison or an arithmetic operator beside a literal made an array
of the literal for every piece and widened the other operand to `long` or `double` first;
a comparison then judged each row through `Option`s, one bit at a time. `Id % 7` divided
in every row. The kept runs of a filter's mask were walked from run to run, a wrong branch
guess per run when runs are short.

**What changed.** Operators with a literal operand loop over the other operand in its own
type: a comparison judges 64 rows into bytes, which the compiler makes vector comparisons
of, and packs them into a word; a remainder by a literal below 2^32 is two
multiplications; a conditional between two computed branches reads its condition a word
at a time. The runs of a mask are read by pairing the starts and ends of the runs of each
word.

**What is left, and why the formulas stop where they do.** The example now prints the new
pages of memory each case is given. The filters are given none. `update_arith` is given
48,830 a run and `update_cond` 19,532: the 200 MB and 80 MB of their results, at 1.8 to
2 microseconds a page on this virtual machine, so about 93 of `update_arith`'s 161 ms and
37 of `update_cond`'s 75 ms are the first write of the result, on any number of threads.
Deephaven writes its results into a heap it already holds. `where_range` on one thread is
now 6 ms of evaluation, a pass over the column for the reserved null value, and about
21 ms making a row set from runs whose blocks are stored as bitmaps in the end; a
constructor of a row set from a bitmap is missing from the contracts. `where_conj` on
one thread is still 1.4 times Deephaven's time: its filters were not changed.

## After task 9.9 (2026-11-17): `aj`, `last_by`, `ungroup`, aggregations with few groups

Task 9.9 (`tasks/9.9-fast-asof-lastby-ungroup.md`) took the cases the second measurement
found slower than Deephaven in both runs, except `where_range`: the as-of join, `last_by`,
`ungroup` at 10M rows, and the low- and medium-cardinality aggregations at 10M rows. Each
was measured inside the engine before it was changed. Results, row order and incremental
behaviour are unchanged.

**How it was measured.** Inside the engine only, release builds of `examples/asof-cases.rs`
(new) and `examples/aggregation-cases.rs` (with a new `ungroup` case: a fresh `head` of a
`group_by` of the source by `K10k`, as the benchmark does). Java was **not** run and the
end-to-end benchmark (`bench/bench.py`) was not run; the Deephaven column below is the
second measurement's, for orientation only. Another agent was building for most of the
task: the one-minute load is given per run, and was 1.3 to 3 for the as-of runs and 4 to
10 for the aggregation runs. Raw output: `bench/results/2026-11-17-task-9.9-*.txt`.

**The as-of join** (`asof-cases`, milliseconds, median; before is `main` at `8a21018b`,
3 repetitions at 1M left rows and 1 at 10M, load 1.3 to 1.6; after 3 repetitions, load 2.8
to 3.1). The right table has a tenth of the left table's rows, as `RA` in the benchmark;
the first row is the benchmark's case.

| case | 1M before | 1M after | 10M before | 10M after | Deephaven, 10M |
| --- | ---: | ---: | ---: | ---: | ---: |
| `aj` on `K10`, `Id >= Stamp` | 449 | 16.9 | 4982 | 230 | 1776 / 1689 |
| the same, then reading the added column | 521 | 24.8 | 5196 | 321 | |
| `Id > Stamp` | 460 | 13.5 | 4824 | 206 | |
| `raj`, `Id <= Stamp` | 529 | 12.9 | | 198 | |
| no exact-match column | 358 | 10.4 | 3553 | 168 | |
| a string key of 1,000 values | 781 | 25.3 | 9988 | 352 | |
| an int stamp in no order | 896 | 99.7 | 15005 | 2281 | |
| refreshing left: the join | 415 | 22.4 | | 212 | |
| then 20 cycles of 1,000 left rows | 12.8 | 11.2 | | 8.3 | |
| refreshing right: the join | 376 | 134 | | 1794 | |
| then 10 cycles of 1,000 right rows | 201 | 162 | | 144 | |

Under callgrind the join spent a third of its instructions putting every left and right
row into an ordered set per key, a sixth finding each left row's match in such a set, and
a sixth in an ordered map of heap-allocated keys, one per row. The keys are now numbered
by the key table of task 9.5 a chunk at a time, stamps of 64 bits or fewer are one number
per row, the right rows of a static right table are one sorted flat array per key in which
each left row's match is found from where the previous left row of its key ended (a merge
when the left stamps are in order, as `Id` is), and left rows are kept per key only when
the right table is refreshing. A refreshing right table still keeps an ordered set per key,
which is why `refreshing right` gained least.

**`last_by`** (`aggregation-cases`, 10,000 keys, six columns). Before, on `main`'s code: 29.5
ms at 1M rows (load 2.0), 339 ms at 10M; after: 18.6 ms at 1M (load 10), 155 to 179 ms at
10M in two rounds (load 4 to 10). Deephaven: 18.2 / 20.1 ms at 1M, 160 / 183 ms at 10M.
Under callgrind most of the work was per row and per operator: each chunk grouped by slot,
then for each of the six operators a call and an effect per group, and the framework noting
each effect. For a static or add-only parent the first and last operators now take a
chunk's rows one at a time in row order and write their marks directly, without the rows
grouped by slot; a row takes its group's place when its row key is beyond the one kept,
which over a chunk comes to what the group's first or last row does.

**`ungroup`** at 10M rows: 3.7 ms before, 0.2 ms after (0.7 to 0.3 at 1M). It was a cost per
element: to learn the size of each vector, `ungroup` reads the grouped column, and the
aggregation's result column answered with a copy of all 10M elements. A read of
consecutive groups of one written array is now a slice of it. The benchmark's 9 ms
included a client round trip of about 1.3 ms (the case takes 1.8 to 2.2 ms end to end at
1M rows against 0.7 here).

**Aggregations with few groups** (10M rows, 3 repetitions, two rounds interleaved; before is
the branch without this change, whose aggregation path for these cases is `main`'s; load
3.6 to 9.6):

| case | before, round 1 / 2 | after, round 1 / 2 | Deephaven (second measurement) |
| --- | ---: | ---: | ---: |
| `sum_by_low` | 190 / 179 | 109 / 113 | 152 / 177 |
| `avg_by_low` | 195 / 190 | 112 / 107 | 156 / 140 |
| `count_by_low` | 154 / 148 | 72 / 67 | 106 / 107 |
| `sum_by_med` | 369 / 385 | 293 / 293 | 344 / 349 |
| `avg_by_med` | 428 / 426 | 373 / 424 | 436 / 522 |
| `count_by_med` | 166 / 158 | 151 / 133 | 151 / 193 |

With 10 keys, grouping each chunk of 4,096 rows by slot (a sort) was 127 instructions per
row under callgrind, more than numbering the keys and the operators together; a chunk whose
slots lie in a narrow range is now grouped by a counting sort. With 10,000 keys nearly every
group of a chunk has one row, and the operators' and the framework's work per group is
most of what is left, as it was for `last_by`; the row-order path of `last_by` does not
apply to operators that read values, and splitting the input over threads was not tried.

**Reading.** Inside the engine, the as-of join of the benchmark's shape is now well under
Deephaven's end-to-end time at both sizes, and `last_by`, `ungroup` and the low-cardinality
aggregations are at or under it; whether that holds end to end needs a run of
`bench/bench.py`.

## After task 9.10, second part (2026-11-17): the row set of a filter, and conjunctions

The second part of task 9.10 (`tasks/9.10-single-thread-filter-formula.md`) took the two
filters the first part left slower than Deephaven on one thread at 10M rows: `where_range`
and `where_conj`. Results, row order and failures are unchanged.

**How it was measured.** Inside the engine only, a release build of `filter-update-cases`
on the machine above, shared with another agent's builds: `-- 10000000 3 1,4` and
`-- 1000000 5 1,4`. "Before" is `main` at `a3be786f`, "after" the branch. The 1M runs and
the 10M "after" run were made one after the other at a load average of 3.2; the 10M
"before" run was made earlier at a load average of 4.9, so its times are high by perhaps a
fifth (the first part measured the same code at 41 and 75 ms). Deephaven was **not** run
again and the end-to-end benchmark (`bench/bench.py`) was not run: the Deephaven column
repeats the second measurement's medians, taken through a server, and is an indication
only. Raw output: `bench/results/2026-11-17-task-9.10b-engine-*.txt`.

Median milliseconds, before / after.

| case | rows | 1 thread | 4 threads | Deephaven, second measurement |
| --- | ---: | ---: | ---: | ---: |
| `where_range` | 1M | 3.7 / 0.9 | 2.8 / 1.2 | 15.6 / 10.1 |
| `where_range` as two filters | 1M | 16.3 / 0.8 | 9.6 / 1.8 | not measured |
| `where_conj` | 1M | 8.8 / 1.7 | 7.6 / 2.0 | 18.8 / 12.1 |
| `where_range` | 10M | 41.2 / 14.3 | 29.9 / 10.3 | 37.0 / 31.3 |
| `where_range` as two filters | 10M | 208 / 10.8 | 63.7 / 5.0 | not measured |
| `where_conj` | 10M | 97.3 / 29.2 | 37.0 / 14.1 | 51.4 / 51.9 |

The steps, one thread, 10M rows, each measured after the one before (load average 1.4 to
1.9 for the last two rows, 4.9 for the first two):

| step | `where_range` | `where_conj` | `K10 = 3` | `B > 50000.0`, 1M rows | `A < 50000`, 650K rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| before | 41.2 | 97.3 | 36.7 | 36.1 | 23.4 |
| a row set from the mask of a contiguous piece | 14.4 | 82.4 | 28.8 | 36.8 | 24.8 |
| match and range kernels in the column's type | 12.5 | 65.3 | 5.4 | 30.8 | 20.0 |
| close scattered rows of a stored column judged whole | 13.0 | 27.9 | 4.8 | 8.5 | 5.0 |

**What changed.**

- `RowSet::from_bitmap` (the one addition to the contracts): a row set from the words of a
  bitmap, a block at a time, in the layout the same runs give. A piece of contiguous rows
  hands its mask to it. A row set per piece of the kept 40%, joined: 26.5 ms from runs,
  0.6 ms from the bitmap.
- The typed kernels of the match, range and between filters reduce the filter to the
  nulls, the stretch of values and the NaN it keeps, and ask each value that in the
  column's type with the packing of `oxhaven_column::bits`; a stretch open at one end is
  one comparison. They judged each cell through an `Option` of a `long` or a `double`.
- A later filter of a conjunction read its column at the rows left (12.8 ms for 1M of 10M
  doubles) and then looked up the key of each kept row. For a column that is an array in
  memory and a filter with a typed kernel, every row from the first selected to the last
  is now judged and the result intersected with the selection: block against block.
  Measured for `B > 50000.0` over one row in *n* of 10M, reading the selected rows against
  judging all: 58.5 / 8.3 ms (*n* = 4), 17.6 / 6.4 (16), 6.0 / 8.2 (64), 1.5 / 6.2 (256),
  0.3 / 6.0 (1024). So all rows are judged while they are at most 32 times the selected
  ones, and the selected rows are read otherwise.
- Scattered rows of any other column that lie close together get their kept bits put at
  their keys in a bitmap, a range of rows at a time, instead of a range per kept stretch.

**What is left.** `where_range` on one thread is now the evaluation of the formula (8 ms),
a pass over the column for the reserved null value, and under a millisecond of row set.
The first write of a new formula result (`update_arith`, `update_cond`) was not touched.
Reading scattered rows of a column that is not an array in memory still walks the ranges
of the selection twice, at about 7 ns a range in the row set's iterator.

## Third measurement (Deephaven on four processors, and both on one thread)

Task 9.11 (`tasks/9.11-benchmark-four-processors.md`) removed the caveat of the first two
measurements: they ran Deephaven with `-XX:ActiveProcessorCount=2` and Oxhaven with 4
threads, so they did not compare like with like. This one measures every case in two
configurations that do: **A**, Deephaven on 4 processors against Oxhaven on 4 threads, and
**B**, both on one (Deephaven with `-XX:ActiveProcessorCount=1`, Oxhaven with
`--threads 1`). It is also the first end-to-end run since tasks 9.9 and 9.10.

The short reading: every case gave the same result on both servers in all four pairs of
runs, and none failed. Of the 74 pairs of case and size, Oxhaven is faster than Deephaven
(ratio below 0.8 in both runs) in 44 in both configurations. In A, 22 are level or between
level and either side, 1 disagrees between the runs, and 7 are slower by more than 20% in
both runs: `agg_by_multi` and `last_by` at 10M rows, `group_by` at both sizes, and the
three high-cardinality aggregations at 1M rows (1.2x to 1.75x). In B, 21 are level or
between, 1 disagrees, and 8 are slower: `agg_by_multi` and `group_by` at both sizes,
`last_by` and `avg_by_med` at 10M rows, and `sum_by_high` and `avg_by_high` at 1M (1.2x
to 1.7x). `aj`, 2x to 3x slower in the second measurement, is now 7x to 14x faster (task
9.9), and `where_range` and `where_conj` are 2x to 16x faster (task 9.10).

**How it was measured.** As the second measurement, with these differences:

- **Oxhaven:** `main` at `c7ff97bc` (which contains tasks 9.9 and 9.10), built with
  `cargo build --release --locked -p oxhaven-server` (3 min 20 s, before any measurement),
  default release profile, without the `python` feature. Run as
  `oxhaven-server --host 127.0.0.1 --port 10869 --psk oxhaven --threads 4` in A and
  `--threads 1` in B; the server logged "Table operations use up to 4 threads" and "... up
  to 1 threads".
- **Deephaven:** 42.9 from `oracle/requirements.txt`, started with
  `oracle/bin/java_server.py --port 10868 --psk oxhaven --heap 5g` as before. That script
  (in `oracle/`, which this task may not change) passes `-XX:ActiveProcessorCount=2` to
  the JVM; the processor count was overridden with the environment variable
  `_JAVA_OPTIONS=-XX:ActiveProcessorCount=4` (A) or `=1` (B), which the JVM reads after
  the options it is started with, so the later value wins. The JVM printed "Picked up
  _JAVA_OPTIONS", and the update graph logged "PeriodicUpdateGraph starting with 4
  notification processing threads" (A) and "with 1" (B). In B a script on the server read
  `Runtime.availableProcessors()` = 1, a 5,120 MB maximum heap and the G1 collector:
  `deephaven_server` passes `-XX:+UseG1GC`, so one processor did not switch the JVM to
  its serial collector. Same 5 GB heap and same warm-up as before (2 untimed and 5 timed
  repetitions at 1M rows, 1 and 3 at 10M). OpenJDK 21.0.12.1.
- **One server at a time.** The earlier measurements ran both servers for the whole run
  and interleaved the cases. This time each server ran alone: started, measured twice
  with `bench.py run` given only that server, stopped, and only then the other started.
  The order was Deephaven A, Oxhaven A, Deephaven B, Oxhaven B. The new command
  `bench.py merge` joins a Deephaven file and an Oxhaven file of the same run into the
  format `run` writes for two servers, and compares the checks of every case exactly as
  `run` does; `report` reads the result. Losing the interleaving means a change of the
  machine's load between the two servers' runs is not shared by both; the load stayed low
  throughout (below).
- **Load:** no build ran during any measurement and no other agent built on the machine.
  One-minute load average 0.88 when the first Deephaven run started (01:59:57) and 1.01
  when the last Oxhaven run ended (02:15:51). Each server's run started at 0.88 to 1.80
  and ended at 1.01 to 1.80; before each case it was 0.89 to 2.07 in A and 0.88 to 1.80
  in B, all of it the benchmark itself (one busy server and the client). A Deephaven run
  took 2 min 8 s to 2 min 32 s, an Oxhaven run about 1 min 30 s.
- **Not pinned to cores.** Neither configuration confines a server to cores (no
  `taskset`): B tells Deephaven's JVM it has one processor, which sizes its thread pools,
  and tells Oxhaven to use one thread per table operation, but the JVM's collector and
  compiler threads and Oxhaven's gRPC runtime still run on any of the 4 cores.

Commands, per server and configuration (A shown; B with `1` in place of `4`):

```bash
_JAVA_OPTIONS=-XX:ActiveProcessorCount=4 python oracle/bin/java_server.py --port 10868 --psk oxhaven --heap 5g &
python bench/bench.py run --server Deephaven=dh+psk://oxhaven@127.0.0.1:10868 --out dh-p4-run1.json
python bench/bench.py run --server Deephaven=dh+psk://oxhaven@127.0.0.1:10868 --out dh-p4-run2.json
# stop Deephaven, then
oxhaven-server --host 127.0.0.1 --port 10869 --psk oxhaven --threads 4 &
python bench/bench.py run --server Oxhaven=dh+psk://oxhaven@127.0.0.1:10869 --out ox-p4-run1.json
python bench/bench.py run --server Oxhaven=dh+psk://oxhaven@127.0.0.1:10869 --out ox-p4-run2.json
python bench/bench.py merge dh-p4-run1.json ox-p4-run1.json --out p4-run1.json   # and run2
python bench/bench.py report p4-run1.json p4-run2.json
```

Raw results: `bench/results/2026-11-18-task-9.11-dh-p4-run1.json` and the like (`dh` or
`ox`, `p4` or `p1`, `run1` or `run2`), one server each; the merged files
`bench/results/2026-11-18-task-9.11-p4-run1.json` and the like; the table with min-max and
load per case of every run is `bench/results/2026-11-18-task-9.11.md`.

Median milliseconds, run 1 / run 2. "ratio" is Oxhaven over Deephaven per run; "ratio,
second measurement" is the same ratio from the second measurement's runs 1 / 2 (Deephaven
on 2 processors, Oxhaven on 4 threads). "same result" is runs 1 / 2.

### A: Deephaven on 4 processors, Oxhaven on 4 threads

#### 1,000,000 rows

| case | Deephaven | Oxhaven | ratio | ratio, second measurement | same result |
| --- | ---: | ---: | ---: | ---: | --- |
| `build` | 194 / 68.1 | 49.2 / 25.0 | 0.25 / 0.37 | 0.21 / 0.36 | yes / yes |
| `update_arith` | 25.9 / 29.9 | 6.7 / 6.0 | 0.26 / 0.20 | 0.21 / 0.39 | yes / yes |
| `update_string` | 30.0 / 25.0 | 6.5 / 9.6 | 0.22 / 0.38 | 0.15 / 0.29 | yes / yes |
| `update_cond` | 21.7 / 15.0 | 3.4 / 3.1 | 0.16 / 0.21 | 0.20 / 0.25 | yes / yes |
| `where_range` | 28.1 / 12.4 | 1.7 / 1.6 | 0.06 / 0.13 | 0.29 / 0.59 | yes / yes |
| `where_string` | 11.8 / 9.4 | 3.4 / 3.9 | 0.29 / 0.41 | 0.22 / 0.18 | yes / yes |
| `where_conj` | 14.4 / 14.8 | 2.1 / 2.4 | 0.15 / 0.16 | 0.44 / 0.74 | yes / yes |
| `sort_int` | 96.8 / 131 | 51.9 / 29.7 | 0.54 / 0.23 | 0.44 / 0.17 | yes / yes |
| `sort_two` | 192 / 210 | 169 / 130 | 0.88 / 0.62 | 0.56 / 0.89 | yes / yes |
| `sort_string` | 203 / 232 | 115 / 114 | 0.56 / 0.49 | 0.46 / 0.47 | yes / yes |
| `sum_by_low` | 21.1 / 19.5 | 15.6 / 14.7 | 0.74 / 0.75 | 1.08 / 1.28 | yes / yes |
| `avg_by_low` | 19.3 / 19.2 | 13.9 / 13.9 | 0.72 / 0.72 | 1.23 / 1.15 | yes / yes |
| `count_by_low` | 15.2 / 14.8 | 10.2 / 9.6 | 0.67 / 0.65 | 0.80 / 2.03 | yes / yes |
| `sum_by_med` | 37.8 / 41.0 | 38.5 / 42.6 | 1.02 / 1.04 | 1.44 / 1.01 | yes / yes |
| `avg_by_med` | 37.3 / 44.8 | 43.0 / 45.2 | 1.15 / 1.01 | 1.13 / 0.83 | yes / yes |
| `count_by_med` | 19.2 / 17.9 | 20.2 / 30.3 | 1.05 / 1.69 | 0.78 / 1.18 | yes / yes |
| `sum_by_high` | 144 / 132 | 194 / 173 | 1.35 / 1.31 | 1.20 / 1.06 | yes / yes |
| `avg_by_high` | 117 / 133 | 194 / 198 | 1.66 / 1.49 | 1.69 / 1.06 | yes / yes |
| `count_by_high` | 95.4 / 93.3 | 115 / 123 | 1.21 / 1.32 | 1.04 / 0.97 | yes / yes |
| `agg_by_multi` | 56.0 / 84.8 | 87.0 / 86.6 | 1.55 / 1.02 | 1.32 / 1.09 | yes / yes |
| `last_by` | 17.7 / 19.5 | 22.7 / 22.2 | 1.28 / 1.14 | 2.16 / 1.97 | yes / yes |
| `natural_join_small` | 23.5 / 22.9 | 10.4 / 13.6 | 0.44 / 0.59 | 0.44 / 0.42 | yes / yes |
| `natural_join_large` | 78.9 / 97.4 | 100 / 112 | 1.27 / 1.15 | 1.02 / 0.93 | yes / yes |
| `exact_join_small` | 20.1 / 22.5 | 10.0 / 12.6 | 0.50 / 0.56 | 0.35 / 0.26 | yes / yes |
| `exact_join_large` | 89.0 / 90.2 | 98.8 / 92.7 | 1.11 / 1.03 | 0.89 / 1.01 | yes / yes |
| `join_on_key` | 233 / 219 | 20.2 / 33.8 | 0.09 / 0.15 | 0.09 / 0.14 | yes / yes |
| `aj` | 173 / 189 | 16.3 / 24.8 | 0.09 / 0.13 | 1.94 / 2.03 | yes / yes |
| `select_distinct_one` | 13.1 / 12.2 | 11.0 / 10.5 | 0.84 / 0.87 | 0.59 / 0.52 | yes / yes |
| `select_distinct_two` | 36.4 / 28.9 | 22.9 / 23.7 | 0.63 / 0.82 | 0.89 / 0.83 | yes / yes |
| `head_half` | 1.8 / 1.6 | 0.93 / 0.73 | 0.53 / 0.47 | 0.39 / 0.52 | yes / yes |
| `tail_half` | 1.9 / 1.5 | 0.71 / 0.85 | 0.38 / 0.57 | 0.40 / 0.49 | yes / yes |
| `group_by` | 67.7 / 62.0 | 81.3 / 93.1 | 1.20 / 1.50 | 0.96 / 1.15 | yes / yes |
| `ungroup` | 4.9 / 2.4 | 0.97 / 1.3 | 0.20 / 0.56 | 0.33 / 0.73 | yes / yes |
| `doget_1m` | 67.3 / 59.6 | 22.5 / 24.9 | 0.33 / 0.42 | 0.40 / 0.46 | yes / yes |
| `sort_refreshing_append` | 43.3 / 16.0 | 34.3 / 34.2 | 0.79 / 2.14 | 0.92 / 1.47 | yes / yes |
| `doget_sorted_refreshing` | 102 / 116 | 46.7 / 44.9 | 0.46 / 0.39 | 0.31 / 0.49 | yes / yes |
| `ticking_append_where_sum_by` | 42.4 / 43.3 | 37.8 / 51.0 | 0.89 / 1.18 | 0.70 / 1.06 | yes / yes |

#### 10,000,000 rows

| case | Deephaven | Oxhaven | ratio | ratio, second measurement | same result |
| --- | ---: | ---: | ---: | ---: | --- |
| `build` | 929 / 860 | 488 / 476 | 0.53 / 0.55 | 0.46 / 0.40 | yes / yes |
| `update_arith` | 176 / 192 | 79.4 / 75.3 | 0.45 / 0.39 | 0.80 / 0.75 | yes / yes |
| `update_string` | 558 / 653 | 91.9 / 104 | 0.16 / 0.16 | 0.21 / 0.12 | yes / yes |
| `update_cond` | 50.8 / 74.1 | 34.4 / 35.7 | 0.68 / 0.48 | 0.74 / 0.96 | yes / yes |
| `where_range` | 30.6 / 30.4 | 8.9 / 8.3 | 0.29 / 0.27 | 1.40 / 1.80 | yes / yes |
| `where_string` | 78.9 / 52.2 | 21.9 / 26.2 | 0.28 / 0.50 | 0.56 / 0.40 | yes / yes |
| `where_conj` | 41.7 / 44.9 | 10.8 / 10.7 | 0.26 / 0.24 | 0.93 / 1.07 | yes / yes |
| `sort_int` | 1334 / 1320 | 601 / 598 | 0.45 / 0.45 | 0.33 / 0.39 | yes / yes |
| `sort_two` | 2311 / 2438 | 1660 / 1709 | 0.72 / 0.70 | 0.70 / 0.64 | yes / yes |
| `sort_string` | 2051 / 2036 | 1640 / 1603 | 0.80 / 0.79 | 0.61 / 0.75 | yes / yes |
| `sum_by_low` | 166 / 185 | 129 / 131 | 0.78 / 0.71 | 1.36 / 1.12 | yes / yes |
| `avg_by_low` | 165 / 181 | 142 / 134 | 0.86 / 0.74 | 1.36 / 1.54 | yes / yes |
| `count_by_low` | 123 / 121 | 96.4 / 119 | 0.78 / 0.98 | 1.49 / 1.43 | yes / yes |
| `sum_by_med` | 312 / 353 | 442 / 394 | 1.42 / 1.11 | 1.22 / 1.24 | yes / yes |
| `avg_by_med` | 318 / 453 | 574 / 408 | 1.80 / 0.90 | 1.19 / 0.95 | yes / yes |
| `count_by_med` | 146 / 188 | 206 / 178 | 1.41 / 0.95 | 1.17 / 0.93 | yes / yes |
| `sum_by_high` | 609 / 745 | 849 / 730 | 1.39 / 0.98 | 0.94 / 0.92 | yes / yes |
| `avg_by_high` | 623 / 749 | 859 / 857 | 1.38 / 1.14 | 0.74 / 0.93 | yes / yes |
| `count_by_high` | 495 / 388 | 464 / 475 | 0.94 / 1.23 | 0.83 / 0.86 | yes / yes |
| `agg_by_multi` | 521 / 606 | 914 / 893 | 1.75 / 1.47 | 1.73 / 1.06 | yes / yes |
| `last_by` | 166 / 146 | 243 / 192 | 1.46 / 1.32 | 2.65 / 2.04 | yes / yes |
| `natural_join_small` | 202 / 197 | 126 / 134 | 0.63 / 0.68 | 0.52 / 0.54 | yes / yes |
| `natural_join_large` | 382 / 370 | 503 / 391 | 1.32 / 1.06 | 1.08 / 0.94 | yes / yes |
| `exact_join_small` | 210 / 206 | 146 / 122 | 0.69 / 0.59 | 0.61 / 0.53 | yes / yes |
| `exact_join_large` | 381 / 350 | 425 / 392 | 1.12 / 1.12 | 0.86 / 0.96 | yes / yes |
| `join_on_key` | 2238 / 2088 | 327 / 293 | 0.15 / 0.14 | 0.15 / 0.16 | yes / yes |
| `aj` | 1747 / 1796 | 203 / 181 | 0.12 / 0.10 | 3.08 / 2.93 | yes / yes |
| `select_distinct_one` | 95.5 / 90.8 | 49.4 / 45.7 | 0.52 / 0.50 | 0.40 / 0.68 | yes / yes |
| `select_distinct_two` | 282 / 249 | 240 / 231 | 0.85 / 0.93 | 0.95 / 0.90 | yes / yes |
| `head_half` | 1.4 / 1.5 | 0.70 / 0.90 | 0.49 / 0.58 | 0.54 / 0.55 | yes / yes |
| `tail_half` | 2.1 / 1.4 | 0.73 / 0.75 | 0.34 / 0.54 | 0.48 / 0.67 | yes / yes |
| `group_by` | 739 / 654 | 914 / 878 | 1.24 / 1.34 | 1.04 / 0.90 | yes / yes |
| `ungroup` | 4.5 / 2.6 | 1.1 / 1.0 | 0.24 / 0.39 | 2.38 / 3.10 | yes / yes |
| `doget_1m` | 65.4 / 64.7 | 41.4 / 23.4 | 0.63 / 0.36 | 0.39 / 0.54 | yes / yes |
| `sort_refreshing_append` | 779 / 793 | 48.8 / 41.1 | 0.06 / 0.05 | 0.05 / 0.04 | yes / yes |
| `doget_sorted_refreshing` | 128 / 128 | 84.9 / 73.0 | 0.66 / 0.57 | 0.48 / 0.39 | yes / yes |
| `ticking_append_where_sum_by` | 110 / 123 | 35.1 / 36.4 | 0.32 / 0.30 | 0.37 / 0.45 | yes / yes |

**A: where Oxhaven is faster** (ratio below 0.8 in both runs), 44 of 74: every formula,
filter and static sort at both sizes, except `sort_two` at 1M rows (0.88 in run 1);
`natural_join_small`, `exact_join_small`, `join_on_key` and `aj` at both sizes; the low-cardinality aggregations at 1M rows and
`sum_by_low` at 10M; `select_distinct_one` at 10M; `head_half`, `tail_half`, `ungroup`,
`doget_1m` and `doget_sorted_refreshing` at both sizes; `sort_refreshing_append` and the
ticking case at 10M.

**A: level** (within 20% in both runs): `sum_by_med` and `avg_by_med` at 1M rows,
`exact_join_large` at both sizes, `select_distinct_one` at 1M, `select_distinct_two` at
10M, the ticking case at 1M. **Between faster and level:** `sort_two` and
`select_distinct_two` at 1M, `avg_by_low` and `count_by_low` at 10M. **Between level and
slower** (one run above 1.2, the other within 20%): `count_by_med` (1.05 / 1.69),
`agg_by_multi` (1.55 / 1.02), `last_by` (1.28 / 1.14) and `natural_join_large` (1.27 /
1.15) at 1M rows; at 10M `natural_join_large` (1.32 / 1.06) and every medium- and
high-cardinality aggregation (`sum_by_med` 1.42 / 1.11, `avg_by_med` 1.80 / 0.90,
`count_by_med` 1.41 / 0.95, `sum_by_high` 1.39 / 0.98, `avg_by_high` 1.38 / 1.14,
`count_by_high` 0.94 / 1.23). **The runs disagree:** `sort_refreshing_append` at 1M rows,
0.79 / 2.14: Oxhaven took 34 ms in both runs, Deephaven 43 and 16.

**A: slower than Deephaven by more than 20% in both runs**, largest first:

| case | rows | ratio | Oxhaven against Deephaven, ms |
| --- | ---: | ---: | --- |
| `agg_by_multi` | 10M | 1.75 / 1.47 | 914 / 893 against 521 / 606 |
| `avg_by_high` | 1M | 1.66 / 1.49 | 194 / 198 against 117 / 133 |
| `last_by` | 10M | 1.46 / 1.32 | 243 / 192 against 166 / 146 |
| `group_by` | 1M | 1.20 / 1.50 | 81.3 / 93.1 against 67.7 / 62.0 |
| `sum_by_high` | 1M | 1.35 / 1.31 | 194 / 173 against 144 / 132 |
| `group_by` | 10M | 1.24 / 1.34 | 914 / 878 against 739 / 654 |
| `count_by_high` | 1M | 1.21 / 1.32 | 115 / 123 against 95.4 / 93.3 |

### B: both on one thread

#### 1,000,000 rows

| case | Deephaven | Oxhaven | ratio | same result |
| --- | ---: | ---: | ---: | --- |
| `build` | 266 / 304 | 68.8 / 49.4 | 0.26 / 0.16 | yes / yes |
| `update_arith` | 55.7 / 40.2 | 7.1 / 6.4 | 0.13 / 0.16 | yes / yes |
| `update_string` | 65.4 / 218 | 19.2 / 17.3 | 0.29 / 0.08 | yes / yes |
| `update_cond` | 27.9 / 11.4 | 5.0 / 3.8 | 0.18 / 0.34 | yes / yes |
| `where_range` | 21.2 / 10.3 | 1.5 / 1.8 | 0.07 / 0.18 | yes / yes |
| `where_string` | 17.0 / 15.2 | 8.0 / 8.6 | 0.47 / 0.57 | yes / yes |
| `where_conj` | 19.0 / 13.8 | 2.6 / 2.7 | 0.14 / 0.20 | yes / yes |
| `sort_int` | 89.5 / 113 | 42.7 / 22.4 | 0.48 / 0.20 | yes / yes |
| `sort_two` | 196 / 212 | 139 / 115 | 0.71 / 0.54 | yes / yes |
| `sort_string` | 242 / 257 | 99.8 / 104 | 0.41 / 0.41 | yes / yes |
| `sum_by_low` | 23.1 / 22.7 | 11.7 / 16.9 | 0.51 / 0.74 | yes / yes |
| `avg_by_low` | 19.0 / 20.8 | 12.7 / 13.4 | 0.67 / 0.64 | yes / yes |
| `count_by_low` | 16.0 / 14.1 | 8.9 / 9.0 | 0.55 / 0.64 | yes / yes |
| `sum_by_med` | 41.7 / 32.9 | 38.7 / 37.5 | 0.93 / 1.14 | yes / yes |
| `avg_by_med` | 37.2 / 40.6 | 37.6 / 39.0 | 1.01 / 0.96 | yes / yes |
| `count_by_med` | 17.0 / 17.8 | 17.4 / 18.2 | 1.02 / 1.02 | yes / yes |
| `sum_by_high` | 126 / 121 | 183 / 149 | 1.45 / 1.23 | yes / yes |
| `avg_by_high` | 140 / 116 | 187 / 163 | 1.33 / 1.40 | yes / yes |
| `count_by_high` | 88.8 / 93.8 | 97.1 / 101 | 1.09 / 1.08 | yes / yes |
| `agg_by_multi` | 56.2 / 57.4 | 82.2 / 82.5 | 1.46 / 1.44 | yes / yes |
| `last_by` | 18.3 / 16.6 | 19.8 / 23.4 | 1.08 / 1.41 | yes / yes |
| `natural_join_small` | 24.7 / 21.7 | 8.9 / 8.2 | 0.36 / 0.38 | yes / yes |
| `natural_join_large` | 94.7 / 80.2 | 63.0 / 75.5 | 0.66 / 0.94 | yes / yes |
| `exact_join_small` | 20.9 / 22.8 | 8.2 / 8.6 | 0.39 / 0.38 | yes / yes |
| `exact_join_large` | 101 / 84.2 | 65.8 / 75.9 | 0.65 / 0.90 | yes / yes |
| `join_on_key` | 357 / 239 | 25.5 / 19.3 | 0.07 / 0.08 | yes / yes |
| `aj` | 200 / 195 | 13.7 / 13.7 | 0.07 / 0.07 | yes / yes |
| `select_distinct_one` | 17.4 / 11.8 | 6.2 / 5.5 | 0.36 / 0.47 | yes / yes |
| `select_distinct_two` | 42.3 / 33.2 | 22.3 / 22.8 | 0.53 / 0.69 | yes / yes |
| `head_half` | 1.9 / 1.6 | 0.66 / 0.75 | 0.35 / 0.47 | yes / yes |
| `tail_half` | 1.9 / 1.4 | 0.58 / 1.8 | 0.31 / 1.31 | yes / yes |
| `group_by` | 55.2 / 61.4 | 77.1 / 77.1 | 1.40 / 1.26 | yes / yes |
| `ungroup` | 5.5 / 2.6 | 1.0 / 1.0 | 0.18 / 0.40 | yes / yes |
| `doget_1m` | 66.4 / 54.8 | 19.3 / 23.0 | 0.29 / 0.42 | yes / yes |
| `sort_refreshing_append` | 33.8 / 21.3 | 31.3 / 34.8 | 0.93 / 1.63 | yes / yes |
| `doget_sorted_refreshing` | 110 / 89.6 | 70.8 / 77.2 | 0.64 / 0.86 | yes / yes |
| `ticking_append_where_sum_by` | 53.6 / 45.5 | 31.0 / 33.2 | 0.58 / 0.73 | yes / yes |

#### 10,000,000 rows

| case | Deephaven | Oxhaven | ratio | same result |
| --- | ---: | ---: | ---: | --- |
| `build` | 1848 / 2075 | 881 / 968 | 0.48 / 0.47 | yes / yes |
| `update_arith` | 187 / 258 | 163 / 184 | 0.87 / 0.72 | yes / yes |
| `update_string` | 1390 / 1217 | 198 / 239 | 0.14 / 0.20 | yes / yes |
| `update_cond` | 104 / 79.1 | 81.0 / 83.3 | 0.78 / 1.05 | yes / yes |
| `where_range` | 38.3 / 62.6 | 14.9 / 19.1 | 0.39 / 0.31 | yes / yes |
| `where_string` | 109 / 92.4 | 76.8 / 80.2 | 0.71 / 0.87 | yes / yes |
| `where_conj` | 67.4 / 67.2 | 29.3 / 31.3 | 0.43 / 0.47 | yes / yes |
| `sort_int` | 1503 / 1196 | 533 / 529 | 0.35 / 0.44 | yes / yes |
| `sort_two` | 2382 / 2297 | 1676 / 1581 | 0.70 / 0.69 | yes / yes |
| `sort_string` | 2211 / 2057 | 1404 / 1492 | 0.63 / 0.73 | yes / yes |
| `sum_by_low` | 269 / 174 | 126 / 130 | 0.47 / 0.75 | yes / yes |
| `avg_by_low` | 163 / 165 | 117 / 132 | 0.72 / 0.80 | yes / yes |
| `count_by_low` | 117 / 115 | 93.3 / 88.1 | 0.80 / 0.76 | yes / yes |
| `sum_by_med` | 306 / 333 | 376 / 395 | 1.23 / 1.19 | yes / yes |
| `avg_by_med` | 311 / 312 | 376 / 435 | 1.21 / 1.40 | yes / yes |
| `count_by_med` | 147 / 193 | 171 / 178 | 1.17 / 0.92 | yes / yes |
| `sum_by_high` | 598 / 658 | 661 / 693 | 1.11 / 1.05 | yes / yes |
| `avg_by_high` | 590 / 622 | 679 / 701 | 1.15 / 1.13 | yes / yes |
| `count_by_high` | 388 / 394 | 378 / 412 | 0.98 / 1.04 | yes / yes |
| `agg_by_multi` | 511 / 528 | 812 / 905 | 1.59 / 1.71 | yes / yes |
| `last_by` | 155 / 142 | 191 / 208 | 1.23 / 1.47 | yes / yes |
| `natural_join_small` | 219 / 226 | 121 / 127 | 0.55 / 0.56 | yes / yes |
| `natural_join_large` | 360 / 373 | 328 / 374 | 0.91 / 1.00 | yes / yes |
| `exact_join_small` | 193 / 264 | 119 / 141 | 0.62 / 0.53 | yes / yes |
| `exact_join_large` | 450 / 369 | 329 / 373 | 0.73 / 1.01 | yes / yes |
| `join_on_key` | 2206 / 2155 | 282 / 318 | 0.13 / 0.15 | yes / yes |
| `aj` | 2087 / 1772 | 185 / 189 | 0.09 / 0.11 | yes / yes |
| `select_distinct_one` | 98.2 / 115 | 47.0 / 49.7 | 0.48 / 0.43 | yes / yes |
| `select_distinct_two` | 233 / 225 | 206 / 291 | 0.88 / 1.30 | yes / yes |
| `head_half` | 1.6 / 1.3 | 0.78 / 0.80 | 0.48 / 0.60 | yes / yes |
| `tail_half` | 2.0 / 1.4 | 0.66 / 0.81 | 0.33 / 0.57 | yes / yes |
| `group_by` | 633 / 613 | 896 / 1042 | 1.41 / 1.70 | yes / yes |
| `ungroup` | 3.6 / 2.1 | 0.97 / 1.5 | 0.27 / 0.70 | yes / yes |
| `doget_1m` | 56.5 / 53.1 | 25.9 / 24.4 | 0.46 / 0.46 | yes / yes |
| `sort_refreshing_append` | 767 / 851 | 37.9 / 48.5 | 0.05 / 0.06 | yes / yes |
| `doget_sorted_refreshing` | 124 / 189 | 134 / 148 | 1.08 / 0.78 | yes / yes |
| `ticking_append_where_sum_by` | 231 / 205 | 33.7 / 36.6 | 0.15 / 0.18 | yes / yes |

**B: where Oxhaven is faster** (ratio below 0.8 in both runs), 44 of 74: every formula
and filter at 1M rows, and `build`, `update_string`, `where_range` and `where_conj` at
10M; every static sort at both sizes; the low-cardinality aggregations at both sizes;
`natural_join_small`, `exact_join_small`, `join_on_key` and `aj` at both sizes;
`select_distinct_one` at both sizes and `select_distinct_two` at 1M; `head_half`,
`ungroup` and `doget_1m` at both sizes and `tail_half` at 10M; the ticking case at both
sizes; `sort_refreshing_append` at 10M.

**B: level** (within 20% in both runs): `sum_by_med`, `avg_by_med`, `count_by_med` and
`count_by_high` at 1M rows; `count_by_med`, `sum_by_high`, `avg_by_high`, `count_by_high`
and `natural_join_large` at 10M. **Between faster and level:** `natural_join_large`,
`exact_join_large` and `doget_sorted_refreshing` at 1M; `update_arith` (0.87 / 0.72),
`update_cond` (0.78 / 1.05), `where_string` (0.71 / 0.87), `exact_join_large` and
`doget_sorted_refreshing` (1.08 / 0.78) at 10M. **Between level and slower:** `last_by`
at 1M (1.08 / 1.41), `sort_refreshing_append` at 1M (0.93 / 1.63; Deephaven 34 and 21
ms, Oxhaven 31 and 35), `sum_by_med` at 10M (1.23 / 1.19), `select_distinct_two` at 10M
(0.88 / 1.30). **The runs disagree:** `tail_half` at 1M, 0.31 / 1.31, where Oxhaven's
median went from 0.58 to 1.8 ms between the runs.

**B: slower than Deephaven by more than 20% in both runs**, largest first:

| case | rows | ratio | Oxhaven against Deephaven, ms |
| --- | ---: | ---: | --- |
| `agg_by_multi` | 10M | 1.59 / 1.71 | 812 / 905 against 511 / 528 |
| `group_by` | 10M | 1.41 / 1.70 | 896 / 1042 against 633 / 613 |
| `agg_by_multi` | 1M | 1.46 / 1.44 | 82.2 / 82.5 against 56.2 / 57.4 |
| `avg_by_high` | 1M | 1.33 / 1.40 | 187 / 163 against 140 / 116 |
| `last_by` | 10M | 1.23 / 1.47 | 191 / 208 against 155 / 142 |
| `sum_by_high` | 1M | 1.45 / 1.23 | 183 / 149 against 126 / 121 |
| `group_by` | 1M | 1.40 / 1.26 | 77.1 / 77.1 against 55.2 / 61.4 |
| `avg_by_med` | 10M | 1.21 / 1.40 | 376 / 435 against 311 / 312 |

What is slower is the same in both configurations: `agg_by_multi`, `group_by`, `last_by`
and the aggregations with many groups. None of them got faster with more threads on
either server, so the gap is the operators' own work per row and group, not threads.
The second measurement listed `aj`, `ungroup` at 10M rows, `where_range` at 10M and the
low-cardinality aggregations at 10M as slower; tasks 9.9 and 9.10 have made all of them
faster than Deephaven in both configurations. `group_by` at both sizes, and in A
`count_by_high` at 1M rows, were level then and are slower now. Oxhaven's own times
are about what they were (`group_by` at 1M rows 77 to 95 ms then, 77 to 93 now; at 10M
845 to 983 then, 878 to 1042 now); Deephaven's are lower (`group_by` at 1M 80 / 83 ms
then, 55 to 68 now; at 10M 838 / 942 then, 613 to 739 now). Deephaven on one processor
is as fast here as on four, so this is not its processors; it is within the spread this
machine shows for Deephaven between runs (below).

**What the processors change.** Medians at 10M rows, 4 processors against 1, run 1 /
run 2:

| case | Deephaven, 4 | Deephaven, 1 | Oxhaven, 4 | Oxhaven, 1 |
| --- | ---: | ---: | ---: | ---: |
| `build` | 929 / 860 | 1848 / 2075 | 488 / 476 | 881 / 968 |
| `update_arith` | 176 / 192 | 187 / 258 | 79.4 / 75.3 | 163 / 184 |
| `update_string` | 558 / 653 | 1390 / 1217 | 91.9 / 104 | 198 / 239 |
| `update_cond` | 50.8 / 74.1 | 104 / 79.1 | 34.4 / 35.7 | 81.0 / 83.3 |
| `where_range` | 30.6 / 30.4 | 38.3 / 62.6 | 8.9 / 8.3 | 14.9 / 19.1 |
| `where_string` | 78.9 / 52.2 | 109 / 92.4 | 21.9 / 26.2 | 76.8 / 80.2 |
| `where_conj` | 41.7 / 44.9 | 67.4 / 67.2 | 10.8 / 10.7 | 29.3 / 31.3 |
| `doget_sorted_refreshing` | 128 / 128 | 124 / 189 | 84.9 / 73.0 | 134 / 148 |
| `ticking_append_where_sum_by` | 110 / 123 | 231 / 205 | 35.1 / 36.4 | 33.7 / 36.6 |

Both servers gain on formulas and filters, Oxhaven by 1.9x to 3.3x and Deephaven by 1.2x
to 2.2x (means of the two runs), so the ratio of a formula or filter is mostly lower in A
than in B; for `build` and `update_string`, where both gain about 2x, it is the same.
Oxhaven also gains on `doget_sorted_refreshing` (a scattered read of a refreshing column
is gathered on several threads), Deephaven on the ticking case (its update graph runs
listeners on its processors). Sorts, joins, aggregations and `group_by` are not faster on
4 on either server. Several of Oxhaven's are 10% to 25% slower in A than in B at 10M
rows: `sort_int` 601 / 598 ms against 533 / 529, `sort_string` 1640 / 1603 against
1404 / 1492, `sum_by_high` 849 / 730 against 661 / 693, `avg_by_high` 859 / 857 against
679 / 701; why was not looked into. Deephaven on 4 processors is about as fast as it
was on 2 in the second measurement: `build` and `update_string` at 10M rows are faster
(929 / 860 against 1393 / 1305 ms, 558 / 653 against 782 / 691), `update_arith` is not
(176 / 192 against 129 / 144), and the filters are within the noise. So the caveat of
the first two measurements moved few of their ratios; most of the change from the second
measurement is tasks 9.9 and 9.10.

**Limits of this measurement.**

- One machine with 4 vCPUs, shared by the server under test and the client, which takes
  a core while it receives a DoGet. Nothing else heavy ran, but this is a virtual
  machine whose host is shared.
- Two runs per server and configuration, 5 timed repetitions at 1M rows and 3 at 10M;
  each median is of few samples. Between run 1 and run 2 the median of the same case
  differs by a median of 9% to 14% for both servers, and by more than 30% for 12
  (Deephaven) and 11 (Oxhaven) of the 74 pairs in A and 19 and 8 in B. The largest
  differences are Deephaven's at 1M rows, up to 3.3x (`build` 194 ms in run 1 against 68
  in run 2 in A, `update_string` 65 against 218 in B), and Oxhaven's `build` at 1M rows
  in A (49 against 25 ms) and `tail_half` at 1M in B (0.58 against 1.8 ms). Within one case, the slowest timed repetition is 1.17 to 1.19 times
  the fastest at the median, and up to 12.7 times for Deephaven (`where_range` at 1M rows
  in B). A ratio up to about 1.3 in one run can be this noise, which is why the lists
  above ask both runs to agree; a case under about 10 ms is mostly round trips.
- The two servers were measured minutes apart, not interleaved case by case, so a slow
  minute of the machine counts against only one of them; the load before each case was
  0.9 to 2.1 throughout.
- Neither server was pinned to cores, so "one thread" limits the table operations and the
  JVM's sizing, not the process (see above). The JVM ran with 2 warm-up repetitions per
  case and two runs per process, so its numbers are close to steady state; its cold
  start is not measured.
